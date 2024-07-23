%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_snowflake_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-elvis([{elvis_text_style, line_length, #{skip_comments => whole_line}}]).

%% -import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_bridge_snowflake.hrl").

-define(USERNAME, <<"admin">>).
-define(PASSWORD, <<"public">>).
-define(BUCKET, <<"mqtt">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case os:getenv("SNOWFLAKE_ACCOUNT_ID") of
        false ->
            %% Have to mock snowflake...
            {skip, todo};
        AccountId ->
            Server = iolist_to_binary([AccountId, ".snowflakecomputing.com"]),
            Username = os:getenv("SNOWFLAKE_USERNAME"),
            Password = os:getenv("SNOWFLAKE_PASSWORD"),
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_snowflake,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {apps, Apps},
                {account_id, AccountId},
                {server, Server},
                {username, Username},
                {password, Password}
                | Config
            ]
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(16)),
    AccountId = ?config(account_id, Config0),
    Server = ?config(server, Config0),
    Username = ?config(username, Config0),
    Password = ?config(password, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, AccountId, Server, Username, Password),
    ActionConfig0 = aggregated_action_config(#{connector => Name}),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(?ACTION_TYPE_BIN, Name, ActionConfig0),
    [
        {bridge_kind, action},
        {action_type, ?ACTION_TYPE_BIN},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig}
        | Config0
    ].

end_per_testcase(_Testcase, _Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, AccountId, Server, Username, Password) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"server">> => Server,
            <<"username">> => Username,
            <<"password">> => Password,
            <<"account">> => AccountId,
            <<"dsn">> => <<"snowflake">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, Name, InnerConfigMap0).

aggregated_action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                  <<"mode">> => <<"aggregated">>,
                  <<"aggregation">> => #{
                                         <<"container">> => #{<<"type">> => <<"csv">>},
                                         <<"time_interval">> => <<"5s">>,
                                         <<"max_records">> => 10
                                        },
                  <<"private_key">> => private_key(),
                  <<"database">> => <<"testdatabase">>,
                  <<"schema">> => <<"public">>,
                  <<"pipe">> => <<"testpipe">>,
                  <<"pipe_user">> => <<"snowpipeuser">>,
                  <<"connect_timeout">> => <<"5s">>,
                  <<"pipelining">> => 100,
                  <<"pool_size">> => 1,
                  <<"max_retries">> => 3
                },
            <<"resource_opts">> => #{
                %% Batch is not yet supported
                %% <<"batch_size">> => 1,
                %% <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"1s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"15s">>,
                <<"resume_interval">> => <<"1s">>,
                <<"worker_pool_size">> => 1
            }
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

private_key() ->
    list_to_binary(os:getenv("SNOWFLAKE_PRIVATE_KEY", "dummy_secret")).

auth_header() ->
    BasicAuth = base64:encode(<<?USERNAME/binary, ":", ?PASSWORD/binary>>),
    {<<"Authorization">>, [<<"Basic ">>, BasicAuth]}.

ensure_scope(Scope, Config) ->
    case get_scope(Scope, Config) of
        {ok, _} ->
            ct:pal("scope ~s already exists", [Scope]),
            ok;
        undefined ->
            ct:pal("creating scope ~s", [Scope]),
            {200, _} = create_scope(Scope, Config),
            ok
    end.

ensure_collection(Scope, Collection, Opts, Config) ->
    case get_collection(Scope, Collection, Config) of
        {ok, _} ->
            ct:pal("collection ~s.~s already exists", [Scope, Collection]),
            ok;
        undefined ->
            ct:pal("creating collection ~s.~s", [Scope, Collection]),
            {200, _} = create_collection(Scope, Collection, Opts, Config),
            ok
    end.

create_scope(Scope, Config) ->
    AdminPool = ?config(admin_pool, Config),
    ReqBody = [<<"name=">>, Scope],
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes">>],
        [
            auth_header(),
            {<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}
        ],
        ReqBody
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, StatusCode, _Headers, Body0} = ehttpc:request(
        AdminPool, post, Request, RequestTTL, MaxRetries
    ),
    Body = maybe_decode_json(Body0),
    ct:pal("create scope response:\n  ~p", [{StatusCode, Body}]),
    {StatusCode, Body}.

create_collection(Scope, Collection, _Opts, Config) ->
    AdminPool = ?config(admin_pool, Config),
    ReqBody = [<<"name=">>, Collection],
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes/">>, Scope, <<"/collections">>],
        [
            auth_header(),
            {<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}
        ],
        ReqBody
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, StatusCode, _Headers, Body0} = ehttpc:request(
        AdminPool, post, Request, RequestTTL, MaxRetries
    ),
    Body = maybe_decode_json(Body0),
    ct:pal("create collection response:\n  ~p", [{StatusCode, Body}]),
    {StatusCode, Body}.

delete_scope(Scope, Config) ->
    AdminPool = ?config(admin_pool, Config),
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes/">>, Scope],
        [auth_header()]
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, StatusCode, _Headers, Body0} = ehttpc:request(
        AdminPool, delete, Request, RequestTTL, MaxRetries
    ),
    Body = maybe_decode_json(Body0),
    ct:pal("delete scope response:\n  ~p", [{StatusCode, Body}]),
    {StatusCode, Body}.

get_scopes(Config) ->
    AdminPool = ?config(admin_pool, Config),
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes">>],
        [auth_header()]
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, 200, _Headers, Body0} = ehttpc:request(AdminPool, get, Request, RequestTTL, MaxRetries),
    Body = maybe_decode_json(Body0),
    ct:pal("get scopes response:\n  ~p", [Body]),
    Body.

get_scope(Scope, Config) ->
    #{<<"scopes">> := Scopes} = get_scopes(Config),
    fetch_with_name(Scopes, Scope).

get_collections(Scope, Config) ->
    maybe
        {ok, #{<<"collections">> := Cs}} = get_scope(Scope, Config),
        {ok, Cs}
    end.

get_collection(Scope, Collection, Config) ->
    maybe
        {ok, Cs} = get_collections(Scope, Config),
        fetch_with_name(Cs, Collection)
    end.

fetch_with_name(Xs, Name) ->
    case [X || X = #{<<"name">> := N} <- Xs, N =:= Name] of
        [] ->
            undefined;
        [X] ->
            {ok, X}
    end.

maybe_decode_json(Body) ->
    case emqx_utils_json:safe_decode(Body, [return_maps]) of
        {ok, JSON} ->
            JSON;
        {error, _} ->
            Body
    end.

%% Collection creation is async...  Trying to insert or select from a recently created
%% collection might result in error 12003, "Keyspace not found in CB datastore".
%% https://www.snowflake.com/forums/t/error-creating-primary-index-immediately-after-collection-creation-keyspace-not-found-in-cb-datastore/32479
wait_until_collection_is_ready(Scope, Collection, Config) ->
    wait_until_collection_is_ready(Scope, Collection, Config, _N = 5, _SleepMS = 200).

wait_until_collection_is_ready(Scope, Collection, _Config, N, _SleepMS) when N < 0 ->
    error({collection_not_ready_timeout, Scope, Collection});
wait_until_collection_is_ready(Scope, Collection, Config, N, SleepMS) ->
    case get_all_rows(Scope, Collection, Config) of
        {ok, _} ->
            ct:pal("collection ~s.~s ready", [Scope, Collection]),
            ok;
        Resp ->
            ct:pal("waiting for collection ~s.~s error response:\n  ~p", [Scope, Collection, Resp]),
            ct:sleep(SleepMS),
            wait_until_collection_is_ready(Scope, Collection, Config, N - 1, SleepMS)
    end.

scope() ->
    <<"some_scope">>.

sql(Name) ->
    Scope = scope(),
    iolist_to_binary([
        <<"insert into default:mqtt.">>,
        Scope,
        <<".">>,
        <<"`">>,
        Name,
        <<"`">>,
        <<" (key, value) values (${.id}, ${.})">>
    ]).

get_all_rows(Scope, Collection, Config) ->
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(Config),
    SQL = iolist_to_binary([
        <<"select * from default:mqtt.">>,
        Scope,
        <<".">>,
        <<"`">>,
        Collection,
        <<"`">>
    ]),
    Opts = #{},
    Resp = emqx_bridge_snowflake_connector:query(ConnResId, SQL, Opts),
    ct:pal("get rows response:\n  ~p", [Resp]),
    case Resp of
        {ok, #{
            status_code := 200, body := #{<<"status">> := <<"success">>, <<"results">> := Rows0}
        }} ->
            Rows = lists:map(
                fun(#{Collection := Value}) ->
                    maybe_decode_json(Value)
                end,
                Rows0
            ),
            {ok, Rows};
        {error, _} ->
            Resp
    end.

proplist_update(Proplist, K, Fn) ->
    {K, OldV} = lists:keyfind(K, 1, Proplist),
    NewV = Fn(OldV),
    lists:keystore(K, 1, Proplist, {K, NewV}).

pre_publish_fn(Scope, Collection, Config) ->
    fun(Context) ->
        ensure_scope(Scope, Config),
        ensure_collection(Scope, Collection, _Opts = #{}, Config),
        wait_until_collection_is_ready(Scope, Collection, Config),
        Context
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "snowflake_connector_stop"),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.
