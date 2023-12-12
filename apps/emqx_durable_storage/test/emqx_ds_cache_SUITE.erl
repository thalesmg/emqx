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
-module(emqx_ds_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    Apps = emqx_cth_suite:start(
        [mria, emqx_durable_storage],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

db_opts() ->
    #{
        backend => builtin,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => 1,
        replication_factor => 3
    }.

open_db(DB) ->
    open_db(DB, db_opts()).

open_db(DB, Opts) ->
    ok = emqx_ds:open_db(DB, Opts),
    on_exit(fun() -> ok = emqx_ds:drop_db(DB) end),
    ok.

consecutive_messages(Opts) ->
    #{
        n := N,
        topic := Topic
    } = Opts,
    StartTime = maps:get(start_time, Opts, erlang:system_time(millisecond)),
    StartNum = maps:get(start_num, Opts, 1),
    lists:map(
        fun(M) ->
            Msg = emqx_message:make(Topic, integer_to_binary(M)),
            Msg#message{timestamp = StartTime + M - StartNum}
        end,
        lists:seq(StartNum, StartNum + N - 1)
    ).

shift_times(TShift, Messages0) ->
    {Messages, _} =
        lists:mapfoldl(
            fun(Msg = #message{timestamp = T0}, T) ->
                {Msg#message{timestamp = T0 + T}, T + 1}
            end,
            TShift,
            Messages0
        ),
    Messages.

get_block_start_time([#message{timestamp = StartTime} | _]) ->
    StartTime.

get_block_end_time(Msgs) ->
    #message{timestamp = StartTime} = lists:last(Msgs),
    StartTime.

consume(DB, TopicFilter, StartTime, Opts) ->
    #{n := N} = Opts,
    BatchSize = maps:get(batch_size, Opts, N),
    AttemptsPerNext = maps:get(attempts, Opts, 4),
    [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    do_consume(DB, Iter, BatchSize, N, {AttemptsPerNext, AttemptsPerNext}).

do_consume(_DB, _Iter0, _BatchSize, N, _AttemptsPerNext) when N =< 0 ->
    ct:pal("~p>>>>>>>\n  ~p", [{?MODULE, ?LINE}, #{}]),
    [];
do_consume(_DB, _Iter0, _BatchSize, _N, _AttemptsPerNext = {0, _}) ->
    ct:pal("~p>>>>>>>\n  ~p", [{?MODULE, ?LINE}, #{}]),
    [];
do_consume(DB, Iter0, BatchSize, N, {AttemptsPerNext, DefaultRetries}) ->
    Size = max(BatchSize, N - BatchSize),
    case emqx_ds:next(DB, Iter0, Size) of
        {ok, Iter, []} ->
            ct:pal("~p>>>>>>>\n  ~p", [{?MODULE, ?LINE}, #{}]),
            ct:sleep(100),
            do_consume(DB, Iter, BatchSize, N, {AttemptsPerNext - 1, DefaultRetries});
        {ok, Iter, Batch} ->
            ct:pal("~p>>>>>>>\n  ~p", [{?MODULE, ?LINE}, #{}]),
            NumBatch = length(Batch),
            Batch ++ do_consume(DB, Iter, BatchSize, N - NumBatch, {DefaultRetries, DefaultRetries})
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_cache_holes_1(_Config) ->
    %% ----------------------------------------------->
    %%                        V
    %% --------|xxxxxxxx|--------|yyyyyyy|-------------->
    %%             c1                c2
    DB = ?FUNCTION_NAME,
    ok = open_db(DB),
    ?check_trace(
        begin
            Topic = <<"some/topic">>,

            TShift1 = 0,
            Messages1 = shift_times(TShift1, [
                emqx_message:make(Topic, <<"1">>),
                emqx_message:make(Topic, <<"2">>),
                emqx_message:make(Topic, <<"3">>)
            ]),
            ok = emqx_ds:store_batch(DB, Messages1),
            StartTime1 = get_block_start_time(Messages1),

            TShift2 = 100_000,
            Messages2 = shift_times(TShift2, [
                emqx_message:make(Topic, <<"4">>),
                emqx_message:make(Topic, <<"5">>),
                emqx_message:make(Topic, <<"6">>)
            ]),
            ok = emqx_ds:store_batch(DB, Messages2),
            StartTime2 = get_block_start_time(Messages2),

            TopicFilter = <<"some/+">>,
            %% fetch 2nd batch first
            Fetched2 = consume(DB, TopicFilter, StartTime2, #{n => 3}),
            Fetched1 = consume(DB, TopicFilter, StartTime1, #{n => 3}),

            ?assertEqual(Messages1, [Msg || {_, Msg} <- Fetched1]),
            ?assertEqual(Messages2, [Msg || {_, Msg} <- Fetched2]),

            Fetched3 = consume(DB, TopicFilter, StartTime1, #{n => 6}),
            ?assertEqual(Messages1 ++ Messages2, [Msg || {_, Msg} <- Fetched3]),

            %% ?assert(false, #{ fetched1 => Fetched1, fetched2 => Fetched2
            %%                 , startt1 => StartTime1, startt2 => StartTime2
            %%                 , cache => emqx_ds_cache:dump_cache(DB), meta => emqx_ds_cache:dump_meta(DB)
            %%                 }),

            ok
        end,
        []
    ),
    ok.

t_cache_holes_2(_Config) ->
    %% ----------------------------------------------->
    %%                        V
    %% --------------|xxxxxxxx|yyyyyyy|-------------->
    %%                  c1        c2
    DB = ?FUNCTION_NAME,
    ok = open_db(DB),
    ?check_trace(
        begin
            Topic = <<"some/topic">>,

            StartTime1 = 1_000,
            Messages1 = consecutive_messages(#{
                n => 3,
                topic => Topic,
                start_time => StartTime1
            }),
            ok = emqx_ds:store_batch(DB, Messages1),

            StartTime2 = get_block_end_time(Messages1) + 1,
            Messages2 = consecutive_messages(#{
                n => 3,
                topic => Topic,
                start_num => 4,
                start_time => StartTime2
            }),
            ok = emqx_ds:store_batch(DB, Messages2),

            TopicFilter = <<"some/+">>,
            %% fetch 2nd batch first
            %% TODO: switch as well
            Fetched2 = consume(DB, TopicFilter, StartTime2, #{n => 3}),
            Fetched1 = consume(DB, TopicFilter, StartTime1, #{n => 3}),

            ?assertEqual(Messages1, [Msg || {_, Msg} <- Fetched1]),
            ?assertEqual(Messages2, [Msg || {_, Msg} <- Fetched2]),

            Fetched3 = consume(DB, TopicFilter, StartTime1, #{n => 6}),
            ?assertEqual(Messages1 ++ Messages2, [Msg || {_, Msg} <- Fetched3]),

            ok
        end,
        []
    ),
    ok.

t_cache_holes_3(_Config) ->
    %% ----------------------------------------------->
    %%                        V
    %% ---------------|xxxxxxxxXXXyyyyy|-------------->
    %%                   c1       c2
    ?check_trace(
        begin
            ok
        end,
        []
    ),
    ct:fail("todo"),
    ok.
