%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_snowflake_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-elvis([{elvis_text_style, line_length, #{skip_comments => whole_line}}]).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_bridge_snowflake.hrl").

%%------------------------------------------------------------------------------
%% Definitions
%%------------------------------------------------------------------------------

-define(DATABASE, <<"testdatabase">>).
-define(SCHEMA, <<"public">>).
-define(STAGE, <<"teststage0">>).
-define(TABLE, <<"test1">>).
-define(WAREHOUSE, <<"testwarehouse">>).
-define(PIPE_USER, <<"snowpipeuser">>).

-define(CONF_COLUMN_ORDER, ?CONF_COLUMN_ORDER([])).
-define(CONF_COLUMN_ORDER(T), [
    <<"publish_received_at">>,
    <<"clientid">>,
    <<"topic">>,
    <<"payload">>
    | T
]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case os:getenv("SNOWFLAKE_ACCOUNT_ID") of
        false ->
            %% Have to mock snowflake...
            {skip, mocked};
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
    ct:timetrap(timer:seconds(90)),
    AccountId = ?config(account_id, Config0),
    Username = ?config(username, Config0),
    Password = ?config(password, Config0),
    Server = ?config(server, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, AccountId, Server, Username, Password),
    ActionConfig0 = aggregated_action_config(#{connector => Name}),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(?ACTION_TYPE_BIN, Name, ActionConfig0),
    ConnPid = new_odbc_client(Config0),
    Config = [
        {bridge_kind, action},
        {action_type, ?ACTION_TYPE_BIN},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig},
        {odbc_client, ConnPid}
        | Config0
    ],
    %% ok = create_table(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = clear_stage(Config),
    ok = truncate_table(Config),
    stop_odbc_client(Config),
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
            <<"pool_size">> => 1,
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
                                         <<"container">> => #{
                                                              <<"type">> => <<"csv">>,
                                                              <<"column_order">> => ?CONF_COLUMN_ORDER
                                                             },
                                         <<"time_interval">> => <<"5s">>,
                                         <<"max_records">> => 10
                                        },
                  <<"private_key">> => private_key(),
                  <<"database">> => ?DATABASE,
                  <<"schema">> => ?SCHEMA,
                  <<"pipe">> => <<"testpipe0">>,
                  <<"stage">> => ?STAGE,
                  <<"pipe_user">> => ?PIPE_USER,
                  <<"connect_timeout">> => <<"5s">>,
                  <<"pipelining">> => 100,
                  <<"pool_size">> => 1,
                  <<"max_retries">> => 3
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>,
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

aggreg_id(BridgeName) ->
    {?ACTION_TYPE_BIN, BridgeName}.

new_odbc_client(Config) ->
    Username = ?config(username, Config),
    Password = ?config(password, Config),
    Server = ?config(server, Config),
    AccountId = ?config(account_id, Config),
    DSN = <<"snowflake">>,
    Opts = [ {username, Username}
           , {password, Password}
           , {account, AccountId}
           , {server, Server}
           , {dsn, DSN}
           ],
    {ok, ConnPid} = emqx_bridge_snowflake_connector:connect(Opts),
    ConnPid.

stop_odbc_client(Config) ->
    ConnPid = ?config(odbc_client, Config),
    ok = emqx_bridge_snowflake_connector:disconnect(ConnPid).

str(IOList) ->
    emqx_utils_conv:str(iolist_to_binary(IOList)).

fqn(Database, Schema, Thing) ->
    [Database, <<".">>, Schema, <<".">>, Thing].

clear_stage(Config) ->
    ConnPid = ?config(odbc_client, Config),
    SQL = str([ <<"remove @">>
              , fqn(?DATABASE, ?SCHEMA, ?STAGE)
              ]),
    {selected, _Header, _Rows} = Res = odbc:sql_query(ConnPid, SQL),
    ct:pal("~s result:\n  ~p", [?FUNCTION_NAME, Res]),
    ok.

create_table(Config) ->
    ConnPid = ?config(odbc_client, Config),
    SQL = str([ <<"create or replace table ">>
              , fqn(?DATABASE, ?SCHEMA, ?TABLE)
              , <<" (">>
              , <<"clientid string">>
              , <<", topic string">>
              , <<", payload binary">>
              , <<", publish_received_at timestamp_ltz">>
              , <<")">>
              ]),
    {updated, _} = Res = odbc:sql_query(ConnPid, SQL),
    ct:pal("~s result:\n  ~p", [?FUNCTION_NAME, Res]),
    ok.

truncate_table(Config) ->
    ConnPid = ?config(odbc_client, Config),
    SQL = str([ <<"truncate ">>
              , fqn(?DATABASE, ?SCHEMA, ?TABLE)
              ]),
    {updated, _} = Res = odbc:sql_query(ConnPid, SQL),
    ct:pal("~s result:\n  ~p", [?FUNCTION_NAME, Res]),
    ok.

row_to_map(Row0, Headers) ->
    Row1 = tuple_to_list(Row0),
    Row2 = lists:map(
             fun(Str) when is_list(Str) ->
                     emqx_utils_conv:bin(Str);
                (Cell) ->
                     Cell
             end,
             Row1),
    Row = lists:zip(Headers, Row2),
    maps:from_list(Row).

get_all_rows(Config) ->
    ConnPid = ?config(odbc_client, Config),
    SQL0 = str([ <<"use warehouse ">>
               , ?WAREHOUSE
               ]),
    {updated, _} = odbc:sql_query(ConnPid, SQL0),
    SQL1 = str([ <<"select * from ">>
              , fqn(?DATABASE, ?SCHEMA, ?TABLE)
              ]),
    {selected, Headers0, Rows} = Res = odbc:sql_query(ConnPid, SQL1),
    ct:pal("~s result:\n  ~p", [?FUNCTION_NAME, Res]),
    Headers = lists:map(fun list_to_binary/1, Headers0),
    lists:map(fun(R) -> row_to_map(R, Headers) end, Rows).

snk_timetrap() ->
    {CTTimetrap, _} = ct:get_timetrap_info(),
    #{timetrap => max(0, CTTimetrap - 500)}.

mk_message({ClientId, Topic, Payload}) ->
    emqx_message:make(bin(ClientId), bin(Topic), Payload).

publish_messages(MessageEvents) ->
    lists:foreach(fun emqx:publish/1, MessageEvents).

wait_until_processed(ActionResId, BeginMark) ->
    {ok, Res} =
        emqx_bridge_snowflake_connector:insert_report(ActionResId, #{begin_mark => BeginMark}),
    ct:pal("insert report (begin mark ~s):\n  ~p", [BeginMark, Res]),
    case Res of
        #{<<"files">> := [_ | _],
          <<"statistics">> := #{<<"activeFilesCount">> := 0}} ->
            ok;
        _ ->
            ct:sleep(2_000),
            wait_until_processed(ActionResId, BeginMark)
    end.

bin2hex(Bin) ->
    emqx_rule_funcs:bin2hexstr(Bin).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "snowflake_connector_stop"),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    io:put_chars(hocon_pp:do(emqx_config:get_raw([]), #{})),
    ok.

%% Unfortunately, there's no way to use toxiproxy to proxy to the real snowflake, as it
%% requires SSL and the hostname mismatch impedes the connection...
t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

t_aggreg_upload(Config) ->
    ActionName = ?config(action_name, Config),
    AggregId = aggreg_id(ActionName),
    ?check_trace(
        snk_timetrap(),
        begin
            %% Create a bridge with the sample configuration.
            ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
            ActionResId = emqx_bridge_v2_testlib:bridge_id(Config),
            {ok, #{<<"nextBeginMark">> := BeginMark}} =
                emqx_bridge_snowflake_connector:insert_report(ActionResId, #{}),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => <<"SELECT "
                                 "  clientid,"
                                 "  topic,"
                                 %% NOTE: binary columsn in snowflake must be hex-encoded...
                                 "  bin2hexstr(payload) as payload,"
                                 "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') "
                                 "    as publish_received_at "
                                 "FROM 'sf/#'"
                               >>
                    }
                ),
            Messages = lists:map(fun mk_message/1, [
                {<<"C1">>, T1 = <<"sf/a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
                {<<"C2">>, T2 = <<"sf/foo/bar">>, P2 = <<"baz">>},
                {<<"C3">>, T3 = <<"sf/t/42">>, P3 = <<"">>},
                %% Won't match rule filter
                {<<"C4">>, <<"t/42">>, <<"won't appear in results">>}
            ]),
            ok = publish_messages(Messages),
            %% Wait until the delivery is completed.
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
            %% Check the uploaded objects.
            wait_until_processed(ActionResId, BeginMark),
            Rows = get_all_rows(Config),
            [ P1Hex
            , P2Hex
            , _P3Hex
            ] = lists:map(fun bin2hex/1, [P1, P2, P3]),
            ?assertMatch(
               [ #{ <<"CLIENTID">> := <<"C1">>
                  , <<"PAYLOAD">> := P1Hex
                    %% {Day, Time}
                  , <<"PUBLISH_RECEIVED_AT">> := {_, _}
                  , <<"TOPIC">> := T1
                  }
               , #{ <<"CLIENTID">> := <<"C2">>
                  , <<"PAYLOAD">> := P2Hex
                    %% {Day, Time}
                  , <<"PUBLISH_RECEIVED_AT">> := {_, _}
                  , <<"TOPIC">> := T2
                  }
               , #{ <<"CLIENTID">> := <<"C3">>
                  , <<"PAYLOAD">> := null
                    %% {Day, Time}
                  , <<"PUBLISH_RECEIVED_AT">> := {_, _}
                  , <<"TOPIC">> := T3
                  }
               ],
               Rows
              ),
            ok
        end,
        []
    ),
    ok.
