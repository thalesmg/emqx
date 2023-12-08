%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Simple time-based cache for `emqx_durable_storage' application.
%%
%% Each DB has its own cache.
-module(emqx_ds_cache).

-behaviour(gen_server).

%% API
-export([
    start_link/1,

    next/4,
    find_cached/3,
    fetch_more/5,
    fetched/3,

    clear_cache/0
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-type key() :: {_Stream, _DSKey}.
-type row() :: {key(), _InsertionTime, _Message}.
-type state() ::
    #{
        db := emqx_ds:db(),
        rotate_timer := reference()
    }.

-define(REF(DB), {via, gproc, {n, l, {?MODULE, DB}}}).
-define(KEY(STREAM, DSKEY), {STREAM, {DSKEY}}).
-define(EOS_MARKER, []).
-define(EOS(STREAM), {STREAM, ?EOS_MARKER}).

%% state
-record(entry, {
    key,
    inserted_at,
    next_key,
    prev_key,
    message
}).

%% call/cast/info records
-record(fetch_more, {
    %% FIXME
    iterator_info :: map(),
    iterator,
    batch_size,
    fetch_fn :: fun(() -> emqx_ds:next_result())
}).
-record(fetched, {
    %% FIXME
    iterator_info :: map(),
    batch :: [{emqx_ds:message_key(), emqx_types:message()}]
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(DB) ->
    gen_server:start_link(?REF(DB), ?MODULE, #{db => DB}, []).

fetch_more(DB, IterInfo, StorageIter, BatchSize, FetchFn) ->
    gen_server:cast(?REF(DB), #fetch_more{
        iterator_info = IterInfo,
        iterator = StorageIter,
        batch_size = BatchSize,
        fetch_fn = FetchFn
    }).

fetched(DB, IterInfo, Batch) ->
    gen_server:cast(?REF(DB), #fetched{iterator_info = IterInfo, batch = Batch}).

next({DB, Shard}, StorageIter0, BatchSize, FetchFn) ->
    %% TODO: check if cache is enabled for stream?
    case emqx_ds_cache:find_cached({DB, Shard}, StorageIter0, BatchSize) of
        {full, StorageIter, Batch} ->
            {ok, StorageIter, Batch};
        {partial, IterInfo, StorageIter, Batch} ->
            %% FIXME: which size to use???
            MaxBatchSize = 500,
            fetch_more(DB, IterInfo, StorageIter, MaxBatchSize, FetchFn),
            {ok, StorageIter, Batch}
    end.

-spec clear_cache() -> ok.
clear_cache() ->
    error(todo).

%%--------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------

init(Opts) ->
    process_flag(trap_exit, true),
    #{db := DB} = Opts,
    create_table(DB),
    State = #{
        db => DB,
        fetching => #{}
    },
    {ok, State}.

terminate(_Reason, _State = #{db := DB}) ->
    drop_table(DB),
    ok.

handle_call(_Call, _From, State) ->
    {reply, error, State}.

handle_cast(
    #fetch_more{iterator_info = #{stream_id := StreamID}}, State = #{fetching := Fetching}
) when
    is_map_key(StreamID, Fetching)
->
    {noreply, State};
handle_cast(
    #fetch_more{
        iterator_info = IterInfo,
        iterator = StorageIter,
        batch_size = BatchSize,
        fetch_fn = FetchFn
    },
    State0
) ->
    #{stream_id := StreamID} = IterInfo,
    #{db := DB, fetching := Fetching0} = State0,
    Task = fun() ->
        %% FIXME!!!!! how to mark EoS???
        case FetchFn(StorageIter, BatchSize) of
            {ok, _Iter, Batch} ->
                fetched(DB, IterInfo, Batch);
            Error ->
                %% try again later???
                error(#{stream_id => StreamID, error => Error})
        end
    end,
    %% FIXME!!!!!!!!  better handling
    spawn(Task),
    Fetching = Fetching0#{StreamID => true},
    {noreply, State0#{fetching := Fetching}};
handle_cast(
    #fetched{iterator_info = IterInfo = #{stream_id := StreamID}, batch = Batch},
    State0 = #{db := DB, fetching := Fetching0}
) when
    is_map_key(StreamID, Fetching0)
->
    cache_batch(DB, IterInfo, Batch),
    Fetching = maps:remove(StreamID, Fetching0),
    {noreply, State0#{fetching := Fetching}};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------

-define(ptkey(DB), {?MODULE, DB}).
-define(cache(DB), (persistent_term:get(?ptkey(DB)))).

-spec create_table(emqx_ds:db()) -> ok.
create_table(DB) ->
    Tid = ets:new(
        emqx_ds_cache,
        [
            ordered_set,
            public,
            {keypos, #entry.key},
            {read_concurrency, true}
        ]
    ),
    persistent_term:put(?ptkey(DB), Tid),
    ok.

drop_table(DB) ->
    ets:delete(?cache(DB)),
    persistent_term:erase(?ptkey(DB)),
    ok.

-record(fetch_state, {
    table,
    shard_id,
    stream_id,
    original_iterator,
    iterator_info,
    last_seen_ds_key,
    current_cache_key,
    remaining,
    acc
}).

%% -spec find_cached(_Stream, _LastSeenKey, _BatchSize) ->
%%             {full, _Iter, _Batch}
%%           | {partial, _StreamID, _Iter, _Batch}.
find_cached(ShardId = {DB, _Shard}, OldIter, BatchSize) ->
    Table = ?cache(DB),
    #{
        stream_id := StreamID,
        last_seen_key := LastSeenKey
    } = IterInfo = emqx_ds_storage_layer:get_iterator_info(ShardId, OldIter),
    State = #fetch_state{
        table = Table,
        shard_id = ShardId,
        stream_id = StreamID,
        original_iterator = OldIter,
        iterator_info = IterInfo,
        last_seen_ds_key = LastSeenKey,
        current_cache_key = ets:next(Table, ?KEY(StreamID, LastSeenKey)),
        remaining = BatchSize,
        acc = []
    },
    find_cached(State).

find_cached(#fetch_state{
    remaining = 0,
    shard_id = ShardId,
    original_iterator = OldIter,
    last_seen_ds_key = LastSeenDSKey,
    acc = Acc
}) ->
    {ok, It} = emqx_ds_storage_layer:update_iterator(ShardId, OldIter, LastSeenDSKey),
    {full, It, lists:reverse(Acc)};
find_cached(#fetch_state{
    current_cache_key = '$end_of_table',
    stream_id = StreamID,
    shard_id = ShardId,
    original_iterator = OldIter,
    iterator_info = IterInfo0,
    last_seen_ds_key = LastSeenDSKey,
    acc = Acc
}) ->
    IterInfo = IterInfo0#{last_seen_key := LastSeenDSKey},
    {ok, It} = emqx_ds_storage_layer:update_iterator(ShardId, OldIter, LastSeenDSKey),
    {partial, IterInfo, It, lists:reverse(Acc)};
find_cached(
    #fetch_state{
        current_cache_key = ?KEY(StreamID, DSKey) = K,
        stream_id = StreamID,
        table = Table,
        shard_id = ShardId,
        original_iterator = OldIter,
        iterator_info = IterInfo0,
        remaining = Remaining0,
        last_seen_ds_key = LastSeenDSKey0,
        acc = Acc0
    } = State0
) ->
    case ets:lookup(Table, K) of
        [#entry{prev_key = {first, _} = Marker, next_key = NextDSKey, message = Msg}] ->
            ExpectedMarker = first_entry_marker(IterInfo0),
            case Marker =:= ExpectedMarker of
                true ->
                    Acc = [{DSKey, Msg} | Acc0],
                    Remaining = Remaining0 - 1,
                    State = State0#fetch_state{
                        acc = Acc,
                        last_seen_ds_key = DSKey,
                        remaining = Remaining,
                        current_cache_key = ?KEY(StreamID, NextDSKey)
                    },
                    find_cached(State);
                false ->
                    IterInfo = IterInfo0#{last_seen_key := LastSeenDSKey0},
                    {ok, It} = emqx_ds_storage_layer:update_iterator(
                        ShardId, OldIter, LastSeenDSKey0
                    ),
                    {partial, IterInfo, It, lists:reverse(Acc0)}
            end;
        [#entry{prev_key = LastSeenDSKey0, next_key = NextDSKey, message = Msg}] ->
            Acc = [{DSKey, Msg} | Acc0],
            Remaining = Remaining0 - 1,
            State = State0#fetch_state{
                acc = Acc,
                last_seen_ds_key = DSKey,
                remaining = Remaining,
                current_cache_key = ?KEY(StreamID, NextDSKey)
            },
            find_cached(State);
        _ ->
            %% Either there's no next entry, or the next entry is not the expected
            %% next message (there's a hole in the cache)
            IterInfo = IterInfo0#{last_seen_key := LastSeenDSKey0},
            {ok, It} = emqx_ds_storage_layer:update_iterator(
                ShardId, OldIter, LastSeenDSKey0
            ),
            {partial, IterInfo, It, lists:reverse(Acc0)}
    end;
find_cached(#fetch_state{
    shard_id = ShardId,
    original_iterator = OldIter,
    iterator_info = IterInfo0,
    last_seen_ds_key = LastSeenDSKey,
    acc = Acc
}) ->
    %% The current cache key belongs to a different stream.
    IterInfo = IterInfo0#{last_seen_key := LastSeenDSKey},
    {ok, It} = emqx_ds_storage_layer:update_iterator(ShardId, OldIter, LastSeenDSKey),
    {partial, IterInfo, It, lists:reverse(Acc)}.

cache_batch(_DB, _IterInfo, _Batch = []) ->
    ok;
cache_batch(DB, IterInfo, Batch = [_First | Tail]) ->
    %% We must store a pointer to the immediate next key on each entry to be able to
    %% detect holes in the cache.
    Now = now_ms(),
    #{last_seen_key := PreviousLastSeenKey} = IterInfo,
    Entries0 = to_entries(IterInfo, Now, PreviousLastSeenKey, Batch, Tail),
    Entries = maybe_fuse_with_previous(DB, PreviousLastSeenKey, Entries0),
    ets:insert(?cache(DB), Entries),
    ok.

now_ms() ->
    erlang:system_time(millisecond).

to_entries(IterInfo = #{stream_id := StreamID}, Now, PrevDSKey, [{DSKey, Msg} | Rest1], [
    {NextDSKey, _} | Rest2
]) ->
    [
        #entry{
            key = ?KEY(StreamID, DSKey),
            inserted_at = Now,
            next_key = NextDSKey,
            prev_key = maybe_first_entry_marker(PrevDSKey, IterInfo),
            message = Msg
        }
        | to_entries(IterInfo, Now, DSKey, Rest1, Rest2)
    ];
to_entries(IterInfo = #{stream_id := StreamID}, Now, PrevDSKey, [{DSKey, Msg}], []) ->
    [
        #entry{
            key = ?KEY(StreamID, DSKey),
            inserted_at = Now,
            next_key = undefined,
            prev_key = maybe_first_entry_marker(PrevDSKey, IterInfo),
            message = Msg
        }
    ].

maybe_first_entry_marker(PreviousLastSeenKey, _IterInfo) when is_binary(PreviousLastSeenKey) ->
    PreviousLastSeenKey;
maybe_first_entry_marker(_PreviousLastSeenKey, IterInfo) ->
    first_entry_marker(IterInfo).

first_entry_marker(IterInfo) ->
    %% This is the first item of the first batch of an iterator.  We need to store a
    %% marker to tie this entry to such iterator when trying to fetch this batch later.
    #{
        topic_filter := TopicFilter,
        start_time := StartTime
    } = IterInfo,
    {first, erlang:md5(term_to_binary({TopicFilter, StartTime}))}.

maybe_fuse_with_previous(_DB, _PreviousLastSeenKey = undefined, Entries) ->
    Entries;
maybe_fuse_with_previous(
    DB, PreviousLastSeenKey, [#entry{key = ?KEY(StreamID, DSKey)} | _] = Entries
) ->
    case ets:lookup(?cache(DB), ?KEY(StreamID, PreviousLastSeenKey)) of
        [#entry{next_key = undefined} = PreviousEntry] ->
            [PreviousEntry#entry{next_key = DSKey} | Entries];
        _ ->
            Entries
    end.

normalize_ds_key(DSKey) when is_binary(DSKey) -> DSKey;
normalize_ds_key(_) -> undefined.
