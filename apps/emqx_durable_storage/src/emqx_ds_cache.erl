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

-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([
    start_link/1,

    find_cached/3,
    fetch_more/3,
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
    message
}).

%% call/cast/info records
-record(fetch_more, {
    stream_id :: emqx_ds:stream_id(),
    fetch_fn :: fun(() -> emqx_ds:next_result())
}).
-record(fetched, {
    stream_id :: emqx_ds:stream_id(),
    batch :: [{emqx_ds:message_key(), emqx_types:message()}]
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(DB) ->
    gen_server:start_link(?REF(DB), ?MODULE, #{db => DB}, []).

fetch_more(DB, StreamID, FetchFn) ->
    gen_server:cast(?REF(DB), #fetch_more{stream_id = StreamID, fetch_fn = FetchFn}).

fetched(DB, StreamID, Batch) ->
    gen_server:cast(?REF(DB), #fetched{stream_id = StreamID, batch = Batch}).

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

handle_cast(#fetch_more{stream_id = StreamID}, State = #{fetching := Fetching}) when
    is_map_key(StreamID, Fetching)
->
    {noreply, State};
handle_cast(#fetch_more{stream_id = StreamID, fetch_fn = FetchFn}, State0) ->
    #{db := DB, fetching := Fetching0} = State0,
    Task = fun() ->
        %% FIXME!!!!! how to mark EoS???
        case FetchFn() of
            {ok, _Iter, Batch} ->
                ?MODULE:fetched(DB, StreamID, Batch);
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
    #fetched{stream_id = StreamID, batch = Batch}, State0 = #{db := DB, fetching := Fetching0}
) when
    is_map_key(StreamID, Fetching0)
->
    cache_batch(DB, StreamID, Batch),
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

%% -spec find_cached(_Stream, _LastSeenKey, _BatchSize) ->
%%             {full, _Iter, _Batch}
%%           | {partial, _Iter, _Batch}.
find_cached(ShardId = {DB, _Shard}, OldIter, BatchSize) ->
    Table = ?cache(DB),
    #{
        stream_id := StreamID,
        last_seen_key := LastSeenKey
    } = emqx_ds_storage_layer:get_iterator_info(ShardId, OldIter),
    find_cached(Table, ShardId, StreamID, OldIter, LastSeenKey, more, BatchSize, []).

find_cached(_Table, ShardId, _StreamID, OldIter, LastSeenKey, _SelectRes, _Remaining = 0, Acc) ->
    {ok, It} = emqx_ds_storage_layer:make_iterator_from_key(ShardId, OldIter, LastSeenKey),
    {full, It, lists:reverse(Acc)};
find_cached(_Table, ShardId, StreamID, OldIter, LastSeenKey0, '$end_of_table', _Remaining, Acc) ->
    {ok, It} = emqx_ds_storage_layer:make_iterator_from_key(ShardId, OldIter, LastSeenKey0),
    {partial, StreamID, It, lists:reverse(Acc)};
find_cached(Table, ShardId, StreamID, OldIter, LastSeenKey0, {[], Cont}, Remaining, Acc) ->
    SelectRes = ets:select(Cont),
    find_cached(Table, ShardId, StreamID, OldIter, LastSeenKey0, SelectRes, Remaining, Acc);
find_cached(Table, ShardId, StreamID, OldIter, _LastSeenKey0, {Msgs, _Cont}, Remaining0, Acc) ->
    NumMsgs = length(Msgs),
    Remaining = Remaining0 - NumMsgs,
    {LastSeenKey, _Msg} = lists:last(Msgs),
    find_cached(Table, ShardId, StreamID, OldIter, LastSeenKey, more, Remaining, Acc ++ Msgs);
find_cached(Table, ShardId, StreamID, OldIter, LastSeenKey0, more, Remaining0, Acc) ->
    MS = find_ms(StreamID, LastSeenKey0),
    SelectRes = ets:select(Table, MS, Remaining0),
    find_cached(Table, ShardId, StreamID, OldIter, LastSeenKey0, SelectRes, Remaining0, Acc).

find_ms(StreamID, LastSeenKey) ->
    ets:fun2ms(
        fun(#entry{key = ?KEY(SID, K), message = Msg}) when
            SID == StreamID andalso K > LastSeenKey
        ->
            {K, Msg}
        end
    ).

cache_batch(DB, StreamID, Batch) ->
    Now = now_ms(),
    ets:insert(?cache(DB), [
        #entry{
            key = ?KEY(StreamID, DSKey),
            inserted_at = Now,
            message = Msg
        }
     || {DSKey, Msg} <- Batch
    ]).

now_ms() ->
    erlang:system_time(millisecond).
