%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("asserts.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    Groups = emqx_limiter_registry:list_groups(),
    lists:foreach(
        fun(Group) ->
            emqx_limiter:delete_group(Group)
        end,
        Groups
    ),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

list_new_groups(ExpectedAlreadyPresent) ->
    emqx_limiter_registry:list_groups() -- ExpectedAlreadyPresent.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Smoke test for adding defaults for a group kind.
t_defaults(_Config) ->
    GroupKind = kind1,
    Group1 = {GroupKind, name1},
    Limiter1 = limiter1,
    Groups0 = emqx_limiter_registry:list_groups(),
    ?assertMatch([], emqx_limiter_registry:list_group_kinds_with_defaults()),
    ?assertError(
        {limiter_not_found, _},
        emqx_limiter_registry:get_limiter_options({Group1, Limiter1})
    ),
    Module = emqx_limiter_exclusive,
    OptsDef = #{capacity => 1, interval => 100, burst_capacity => 0},
    LimiterOptionsDef = [{Limiter1, OptsDef}],
    ok = emqx_limiter_registry:register_group_kind_defaults(GroupKind, Module, LimiterOptionsDef),
    ?assertMatch([], list_new_groups(Groups0)),
    ?assertMatch([GroupKind], emqx_limiter_registry:list_group_kinds_with_defaults()),
    %% Now, even if `Group1' itself is not registered, we have get the defaults
    ?assertEqual(OptsDef, emqx_limiter_registry:get_limiter_options({Group1, Limiter1})),
    %% This limiter is not in the defaults, so we don't get it back.
    Limiter2 = limiter2,
    ?assertError(
        {limiter_not_found, _},
        emqx_limiter_registry:get_limiter_options({Group1, Limiter2})
    ),
    %% Now we register `Group1' proper.
    Opts1 = #{capacity => 2, interval => 200, burst_capacity => 20},
    LimiterOptions1 = [{Limiter1, Opts1}],
    ok = emqx_limiter_registry:register_group(Group1, Module, LimiterOptions1),
    ?assertMatch([Group1], list_new_groups(Groups0)),
    ?assertMatch([GroupKind], emqx_limiter_registry:list_group_kinds_with_defaults()),
    ?assertEqual(Opts1, emqx_limiter_registry:get_limiter_options({Group1, Limiter1})),
    %% Unregistering `Group1' should leave defaults alone
    ok = emqx_limiter_registry:unregister_group(Group1),
    ?assertMatch([], list_new_groups(Groups0)),
    ?assertMatch([GroupKind], emqx_limiter_registry:list_group_kinds_with_defaults()),
    ?assertEqual(OptsDef, emqx_limiter_registry:get_limiter_options({Group1, Limiter1})),
    %% Only when we unregister `GroupKind' defaults we should forget them.
    ok = emqx_limiter_registry:unregister_group_kind_defaults(GroupKind),
    ?assertMatch([], list_new_groups(Groups0)),
    ?assertMatch([], emqx_limiter_registry:list_group_kinds_with_defaults()),
    ?assertError(
        {limiter_not_found, _},
        emqx_limiter_registry:get_limiter_options({Group1, Limiter1})
    ),
    ok.
