%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(START_APPS, [emqx, emqx_conf, emqx_connector]).
-define(CONNECTOR, dummy_connector_impl).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps(?START_APPS),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps(?START_APPS).

init_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase({init, Config}).

end_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase({'end', Config}).

%% the 2 test cases below are based on kafka connector which is ee only
-if(?EMQX_RELEASE_EDITION == ee).
t_connector_lifecycle({init, Config}) ->
    meck:new(emqx_connector_ee_schema, [passthrough]),
    meck:expect(emqx_connector_ee_schema, resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    [{mocked_mods, [?CONNECTOR, emqx_connector_ee_schema]} | Config];
t_connector_lifecycle({'end', Config}) ->
    MockedMods = ?config(mocked_mods, Config),
    meck:unload(MockedMods),
    Config;
t_connector_lifecycle(_Config) ->
    ?assertEqual(
        [],
        emqx_connector:list()
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:create(kafka, my_connector, connector_config())
    ),

    ?assertMatch(
        {ok, #{name := my_connector, type := kafka}},
        emqx_connector:lookup(<<"connector:kafka:my_connector">>)
    ),

    ?assertMatch(
        {ok, #{name := my_connector, type := kafka, resource_data := #{status := connected}}},
        emqx_connector:lookup(<<"kafka:my_connector">>)
    ),

    ?assertMatch(
        {ok, #{name := my_connector, type := kafka, resource_data := #{status := connected}}},
        emqx_connector:lookup(kafka, my_connector)
    ),

    ?assertMatch(
        [#{name := <<"my_connector">>, type := <<"kafka">>}],
        emqx_connector:list()
    ),

    ?assertMatch(
        {ok, #{config := #{enable := false}}},
        emqx_connector:disable_enable(disable, kafka, my_connector)
    ),

    ?assertMatch(
        {ok, #{resource_data := #{status := stopped}}},
        emqx_connector:lookup(kafka, my_connector)
    ),

    ?assertMatch(
        {ok, #{config := #{enable := true}}},
        emqx_connector:disable_enable(enable, kafka, my_connector)
    ),

    ?assertMatch(
        {ok, #{resource_data := #{status := connected}}},
        emqx_connector:lookup(kafka, my_connector)
    ),

    ?assertMatch(
        {ok, #{config := #{connect_timeout := 10000}}},
        emqx_connector:update(kafka, my_connector, (connector_config())#{
            <<"connect_timeout">> => <<"10s">>
        })
    ),

    ?assertMatch(
        {ok, #{resource_data := #{config := #{connect_timeout := 10000}}}},
        emqx_connector:lookup(kafka, my_connector)
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:remove(kafka, my_connector)
    ),

    ?assertEqual(
        [],
        emqx_connector:list()
    ),

    ?assert(meck:validate(?CONNECTOR)),
    ?assertMatch(
        [
            {_, {?CONNECTOR, callback_mode, []}, _},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            {_, {?CONNECTOR, callback_mode, []}, _},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok}
        ],
        meck:history(?CONNECTOR)
    ),
    ok.

t_remove_fail({'init', Config}) ->
    meck:new(emqx_connector_ee_schema, [passthrough]),
    meck:expect(emqx_connector_ee_schema, resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_get_channels, 1, [{<<"my_channel">>, #{}}]),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    [{mocked_mods, [?CONNECTOR, emqx_connector_ee_schema]} | Config];
t_remove_fail({'end', Config}) ->
    MockedMods = ?config(mocked_mods, Config),
    meck:unload(MockedMods),
    Config;
t_remove_fail(_Config) ->
    ?assertEqual(
        [],
        emqx_connector:list()
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:create(kafka, my_failing_connector, connector_config())
    ),

    ?assertMatch(
        {error, {post_config_update, emqx_connector, {active_channels, [{<<"my_channel">>, _}]}}},
        emqx_connector:remove(kafka, my_failing_connector)
    ),

    ?assertNotEqual(
        [],
        emqx_connector:list()
    ),

    ?assert(meck:validate(?CONNECTOR)),
    ?assertMatch(
        [
            {_, {?CONNECTOR, callback_mode, []}, _},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_channels, [_]}, _},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_get_channels, [_]}, _},
            {_, {?CONNECTOR, on_add_channel, _}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_channels, [_]}, _}
        ],
        meck:history(?CONNECTOR)
    ),
    ok.

%% helpers

connector_config() ->
    #{
        <<"authentication">> => <<"none">>,
        <<"bootstrap_hosts">> => <<"127.0.0.1:9092">>,
        <<"connect_timeout">> => <<"5s">>,
        <<"enable">> => true,
        <<"metadata_request_timeout">> => <<"5s">>,
        <<"min_metadata_refresh_interval">> => <<"3s">>,
        <<"socket_opts">> =>
            #{
                <<"recbuf">> => <<"1024KB">>,
                <<"sndbuf">> => <<"1024KB">>,
                <<"tcp_keepalive">> => <<"none">>
            },
        <<"ssl">> =>
            #{
                <<"ciphers">> => [],
                <<"depth">> => 10,
                <<"enable">> => false,
                <<"hibernate_after">> => <<"5s">>,
                <<"log_level">> => <<"notice">>,
                <<"reuse_sessions">> => true,
                <<"secure_renegotiate">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
            }
    }.
-endif.
