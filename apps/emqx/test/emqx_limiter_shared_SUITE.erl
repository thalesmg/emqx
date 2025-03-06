%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_shared_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Groups = emqx_limiter_registry:list_groups(),
    lists:foreach(
        fun(Group) ->
            emqx_limiter:delete_group(Group)
        end,
        Groups
    ),
    Config.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_try_consume(_) ->
    Group = {kind1, group1},
    ok = emqx_limiter:create_group(shared, Group, [
        {limiter0, #{capacity => 2, interval => 100, burst_capacity => 0}},
        {limiter1, #{capacity => 2, interval => 100, burst_capacity => 0}}
    ]),

    %% Create two different clients to consume tokens
    ClientA0 = emqx_limiter:connect({Group, limiter1}),
    ClientB0 = emqx_limiter:connect({Group, limiter1}),

    %% Consume both tokens concurrently
    {true, ClientA1} = emqx_limiter_client:try_consume(ClientA0, 1),
    {true, ClientB1} = emqx_limiter_client:try_consume(ClientB0, 1),
    {false, ClientA2, {failed_to_consume_from_limiter, {Group, limiter1}}} = emqx_limiter_client:try_consume(
        ClientA1, 1
    ),
    {false, ClientB2, {failed_to_consume_from_limiter, {Group, limiter1}}} = emqx_limiter_client:try_consume(
        ClientB1, 1
    ),
    ct:sleep(110),

    %% Capacity should be refilled
    {true, ClientA3} = emqx_limiter_client:try_consume(ClientA2, 1),
    {true, ClientB3} = emqx_limiter_client:try_consume(ClientB2, 1),
    {false, _ClientA4, _} = emqx_limiter_client:try_consume(ClientA3, 1),
    {false, _ClientB4, _} = emqx_limiter_client:try_consume(ClientB3, 1).

t_try_consume_burst(_) ->
    Group = {kind1, group1},
    ok = emqx_limiter:create_group(shared, Group, [
        {limiter1, #{capacity => 2, interval => 100, burst_capacity => 8, burst_interval => 1000}}
    ]),
    Client0 = emqx_limiter:connect({Group, limiter1}),

    %% Consume full capacity
    Client1 = lists:foldl(
        fun(_, ClientAcc0) ->
            {true, ClientAcc1} = emqx_limiter_client:try_consume(ClientAcc0, 1),
            ClientAcc1
        end,
        Client0,
        lists:seq(1, 10)
    ),
    {false, Client2, _} = emqx_limiter_client:try_consume(Client1, 1),

    ct:sleep(110),
    %% Only regularly refilled tokens are available
    {true, Client3} = emqx_limiter_client:try_consume(Client2, 1),
    {true, Client4} = emqx_limiter_client:try_consume(Client3, 1),
    {false, Client5, _} = emqx_limiter_client:try_consume(Client4, 1),

    ct:sleep(900),
    %% Burst tokens are available again
    lists:foldl(
        fun(_, ClientAcc0) ->
            {true, ClientAcc1} = emqx_limiter_client:try_consume(ClientAcc0, 1),
            ClientAcc1
        end,
        Client5,
        lists:seq(1, 10)
    ).

t_put_back(_) ->
    Group = {kind1, group1},
    ok = emqx_limiter:create_group(shared, Group, [
        {limiter1, #{capacity => 2, interval => 100, burst_capacity => 0}}
    ]),

    %% Create a client and consume tokens
    Client0 = emqx_limiter:connect({Group, limiter1}),
    {true, Client1} = emqx_limiter_client:try_consume(Client0, 1),
    {true, Client2} = emqx_limiter_client:try_consume(Client1, 1),
    {false, Client3, _} = emqx_limiter_client:try_consume(Client2, 1),

    %% Put back one token
    Client4 = emqx_limiter_client:put_back(Client3, 1),

    %% Check if the token is refilled back
    {true, Client5} = emqx_limiter_client:try_consume(Client4, 1),
    {false, _Client6, _} = emqx_limiter_client:try_consume(Client5, 1).

t_change_options(_) ->
    Group = {kind1, group1},
    ok = emqx_limiter:create_group(shared, Group, [
        {limiter1, #{capacity => 1, interval => 100, burst_capacity => 0}}
    ]),

    %% Create a client and consume tokens
    Client0 = emqx_limiter:connect({Group, limiter1}),
    {true, Client1} = emqx_limiter_client:try_consume(Client0, 1),
    {false, Client2, _} = emqx_limiter_client:try_consume(Client1, 1),

    %% Change the options, increase the capacity and interval
    ok = emqx_limiter:update_group(Group, [
        {limiter1, #{capacity => 2, interval => 200, burst_capacity => 0}}
    ]),

    %% The tokens will be refilled at the end of the NEW interval
    ct:sleep(210),
    {true, Client3} = emqx_limiter_client:try_consume(Client2, 1),
    {true, Client4} = emqx_limiter_client:try_consume(Client3, 1),
    {false, Client5, _} = emqx_limiter_client:try_consume(Client4, 1),

    %% infinite capacity should be applied immediately
    ok = emqx_limiter:update_group(Group, [
        {limiter1, #{capacity => infinity}}
    ]),
    lists:foldl(
        fun(_, ClientAcc0) ->
            {true, ClientAcc} = emqx_limiter_client:try_consume(ClientAcc0, 100),
            ClientAcc
        end,
        Client5,
        lists:seq(1, 10)
    ).

t_change_options_from_unlimited(_) ->
    Group = {kind1, group1},
    %% Create a client and consume tokens
    ok = emqx_limiter:create_group(shared, Group, [
        {limiter1, #{capacity => infinity}}
    ]),
    Client0 = emqx_limiter:connect({Group, limiter1}),
    {true, Client1} = emqx_limiter_client:try_consume(Client0, 1),

    %% Change the options, set finite capacity
    ok = emqx_limiter:update_group(Group, [
        {limiter1, #{capacity => 100, interval => 200, burst_capacity => 0}}
    ]),

    %% Check that the bucket is correctly converted and the client can still consume tokens
    ct:sleep(210),
    {true, _Client2} = emqx_limiter_client:try_consume(Client1, 1).

t_concurrent_high_capacity(_) ->
    ok = test_concurrent(33333, 1000).

t_concurrent_low_capacity(_) ->
    ok = test_concurrent(333, 1000).

test_concurrent(Capacity, Interval) ->
    Group = {kind1, group1},
    ok = emqx_limiter:create_group(shared, Group, [
        {limiter0, #{capacity => Capacity, interval => Interval, burst_capacity => 0}},
        {limiter1, #{capacity => Capacity, interval => Interval, burst_capacity => 0}}
    ]),
    Self = self(),
    TestInterval = 1111,
    Deadline = erlang:monotonic_time(millisecond) + TestInterval,

    %% Let 10 concurrent consumers consume tokens
    lists:foreach(
        fun(_) ->
            Client = emqx_limiter:connect({Group, limiter1}),
            spawn_link(fun() ->
                Consumed = consume_till(Client, Deadline, 0),
                Self ! {consumed, Consumed}
            end)
        end,
        lists:seq(1, 10)
    ),

    %% Wait for the consumers to finish
    ct:sleep(TestInterval + 100),
    Consumed = count_consumed(),
    %% Initial capacity + Generated tokens
    Expected = Capacity + Capacity * TestInterval div Interval,

    %% Verify the consumed tokens are close to the expected value
    RelativeError = abs(Consumed - Expected) / Expected,
    ct:pal("Consumed: ~p, Expected: ~p, Diff: ~p", [
        Consumed, Expected, RelativeError
    ]),
    ?assert(RelativeError < 0.01, #{
        relative_error => RelativeError,
        consumed => Consumed,
        expected => Expected
    }).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

consume_till(Client, Deadline, Consumed) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            Consumed;
        false ->
            case emqx_limiter_client:try_consume(Client, 1) of
                {true, Client1} ->
                    consume_till(Client1, Deadline, Consumed + 1);
                {false, Client1, _} ->
                    consume_till(Client1, Deadline, Consumed)
            end
    end.

count_consumed() ->
    count_consumed(0).

count_consumed(N) ->
    receive
        {consumed, Cnt} ->
            count_consumed(N + Cnt)
    after 100 ->
        N
    end.
