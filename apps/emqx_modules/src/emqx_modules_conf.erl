%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The emqx-modules configuration interoperable interfaces
-module(emqx_modules_conf).

-behaviour(emqx_config_handler).

%% Load/Unload
-export([
    load/0,
    unload/0
]).

%% topci-metrics
-export([
    topic_metrics/0,
    add_topic_metrics/1,
    remove_topic_metrics/1
]).

%% config handlers
-export([
    pre_config_update/3,
    post_config_update/5
]).

%%--------------------------------------------------------------------
%% Load/Unload

-spec load() -> ok.
load() ->
    emqx_conf:add_handler([topic_metrics], ?MODULE).

-spec unload() -> ok.
unload() ->
    emqx_conf:remove_handler([topic_metrics]).

%%--------------------------------------------------------------------
%% Topic-Metrics

-spec topic_metrics() -> [emqx_types:topic()].
topic_metrics() ->
    lists:map(
        fun(#{topic := Topic}) -> Topic end,
        emqx:get_config([topic_metrics])
    ).

-spec add_topic_metrics(emqx_types:topic()) ->
    {ok, emqx_types:topic()}
    | {error, term()}.
add_topic_metrics(Topic) ->
    case cfg_update(topic_metrics, ?FUNCTION_NAME, Topic) of
        {ok, _} -> {ok, Topic};
        {error, Reason} -> {error, Reason}
    end.

-spec remove_topic_metrics(emqx_types:topic()) ->
    ok
    | {error, term()}.
remove_topic_metrics(Topic) ->
    case cfg_update(topic_metrics, ?FUNCTION_NAME, Topic) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

cfg_update(topic_metrics, Action, Params) ->
    res(
        emqx_conf:update(
            [topic_metrics],
            {Action, Params},
            #{override_to => cluster}
        )
    ).

res({ok, Result}) -> {ok, Result};
res({error, {pre_config_update, ?MODULE, Reason}}) -> {error, Reason};
res({error, {post_config_update, ?MODULE, Reason}}) -> {error, Reason};
res({error, Reason}) -> {error, Reason}.

%%--------------------------------------------------------------------
%%  Config Handler
%%--------------------------------------------------------------------

-spec pre_config_update(
    list(atom()),
    emqx_config:update_request(),
    emqx_config:raw_config()
) ->
    {ok, emqx_config:update_request()} | {error, term()}.
pre_config_update(_, {add_topic_metrics, Topic0}, RawConf) ->
    Topic = #{<<"topic">> => Topic0},
    case lists:member(Topic, RawConf) of
        true ->
            {error, already_existed};
        _ ->
            {ok, RawConf ++ [Topic]}
    end;
pre_config_update(_, {remove_topic_metrics, Topic0}, RawConf) ->
    Topic = #{<<"topic">> => Topic0},
    case lists:member(Topic, RawConf) of
        true ->
            {ok, RawConf -- [Topic]};
        _ ->
            {error, not_found}
    end.

-spec post_config_update(
    list(atom()),
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

post_config_update(
    _,
    {add_topic_metrics, Topic},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    case emqx_topic_metrics:register(Topic) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end;
post_config_update(
    _,
    {remove_topic_metrics, Topic},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    case emqx_topic_metrics:deregister(Topic) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.
