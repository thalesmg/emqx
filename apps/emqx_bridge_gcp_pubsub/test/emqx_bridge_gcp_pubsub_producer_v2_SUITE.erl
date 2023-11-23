%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_producer_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

-define(ACTION_TYPE, gcp_pubsub_producer).
-define(ACTION_TYPE_BIN, <<"gcp_pubsub_producer">>).
-define(CONNECTOR_TYPE, gcp_pubsub_producer).
-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_producer">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    GCPEmulatorHost = os:getenv("GCP_EMULATOR_HOST", "toxiproxy"),
    GCPEmulatorPortStr = os:getenv("GCP_EMULATOR_PORT", "8085"),
    GCPEmulatorPort = list_to_integer(GCPEmulatorPortStr),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    ProxyName = "gcp_emulator",
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    case emqx_common_test_helpers:is_tcp_server_available(GCPEmulatorHost, GCPEmulatorPort) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx,
                    emqx_management,
                    emqx_resource,
                    emqx_bridge_gcp_pubsub,
                    emqx_bridge,
                    emqx_rule_engine,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => ?config(priv_dir, Config)}
            ),
            emqx_mgmt_api_test_util:init_suite(),
            HostPort = GCPEmulatorHost ++ ":" ++ GCPEmulatorPortStr,
            true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
            Client = start_control_client(),
            [
                {apps, Apps},
                {proxy_name, ProxyName},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort},
                {gcp_emulator_host, GCPEmulatorHost},
                {gcp_emulator_port, GCPEmulatorPort},
                {client, Client}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_gcp_emulator);
                _ ->
                    {skip, no_gcp_emulator}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    Client = ?config(client, Config),
    stop_control_client(Client),
    emqx_mgmt_api_test_util:end_suite(),
    emqx_cth_suite:stop(Apps),
    persistent_term:erase({emqx_bridge_gcp_pubsub_client, transport}),
    ok.

init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_config:delete_override_conf_files(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    Topic = Name,
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    Config = [{project_id, ProjectId} | Config0],
    ConnectorConfig = connector_config(Name, ServiceAccountJSON),
    {BridgeConfig, ExtraConfig} = action_config(Name, Name, Topic),
    ensure_topic(Config, Topic),
    ok = snabbkaffe:start_trace(),
    ExtraConfig ++
        [
            {connector_type, ?CONNECTOR_TYPE},
            {connector_name, Name},
            {connector_config, ConnectorConfig},
            {bridge_type, ?ACTION_TYPE},
            {bridge_name, Name},
            {bridge_config, BridgeConfig}
            | Config
        ].

end_per_testcase(_TestCase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, ServiceAccountJSON) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"connect_timeout">> => <<"1s">>,
            <<"service_account_json">> => ServiceAccountJSON,
            <<"max_retries">> => 2,
            <<"resource_opts">> => #{
                <<"request_ttl">> => <<"5s">>
            }
        },
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    parse_and_check_connector_config(InnerConfigMap, Name).

parse_and_check_connector_config(InnerConfigMap, Name) ->
    TypeBin = ?CONNECTOR_TYPE_BIN,
    RawConf = #{<<"connectors">> => #{TypeBin => #{Name => InnerConfigMap}}},
    #{<<"connectors">> := #{TypeBin := #{Name := Config}}} =
        hocon_tconf:check_plain(emqx_connector_schema, RawConf, #{
            required => false, atom_key => false
        }),
    ct:pal("parsed config: ~p", [Config]),
    InnerConfigMap.

action_config(Name, ConnectorId, Topic) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> =>
                #{
                    <<"attributes_template">> => [],
                    <<"ordering_key_template">> => <<"${.payload.ok}">>,
                    <<"payload_template">> => <<>>,
                    <<"pubsub_topic">> => Topic
                },
            <<"local_topic">> => <<"t/gcp_produ">>,
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"async">>
            }
        },
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    ExtraConfig =
        [{pubsub_topic, Topic}],
    {parse_and_check_action_config(InnerConfigMap, Name), ExtraConfig}.

%% check it serializes correctly
serde_roundtrip(InnerConfigMap0) ->
    IOList = hocon_pp:do(InnerConfigMap0, #{}),
    {ok, InnerConfigMap} = hocon:binary(IOList),
    InnerConfigMap.

parse_and_check_action_config(InnerConfigMap, Name) ->
    TypeBin = ?ACTION_TYPE_BIN,
    RawConf = #{<<"actions">> => #{TypeBin => #{Name => InnerConfigMap}}},
    #{<<"actions">> := #{TypeBin := #{Name := Config}}} =
        hocon_tconf:check_plain(
            emqx_bridge_v2_schema,
            RawConf,
            #{required => false, atom_key => false}
        ),
    ct:pal("parsed config: ~p", [Config]),
    InnerConfigMap.

make_message() ->
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    #{
        clientid => BinTime,
        payload => Payload,
        timestamp => Time
    }.

start_control_client() ->
    RawServiceAccount = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ConnectorConfig =
        #{
            connect_timeout => 5_000,
            max_retries => 0,
            pool_size => 1,
            resource_opts => #{request_ttl => 5_000},
            service_account_json => RawServiceAccount
        },
    PoolName = <<"control_connector">>,
    {ok, Client} = emqx_bridge_gcp_pubsub_client:start(PoolName, ConnectorConfig),
    Client.

stop_control_client(Client) ->
    ok = emqx_bridge_gcp_pubsub_client:stop(Client),
    ok.

ensure_topic(Config, Topic) ->
    ProjectId = ?config(project_id, Config),
    Client = ?config(client, Config),
    Method = put,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
    Body = <<"{}">>,
    Res = emqx_bridge_gcp_pubsub_client:query_sync(
        {prepared_request, {Method, Path, Body}},
        Client
    ),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 409}} ->
            %% already exists
            ok
    end,
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, gcp_pubsub_stop),
    ok.

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => disconnected}),
    ok.

t_sync_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertMatch({ok, #{status_code := 200}}, Res) end,
        gcp_pubsub_producer_sync
    ),
    ok.

t_async_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_async_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertMatch({ok, #{status_code := 200}}, Res) end,
        gcp_pubsub_producer_async
    ),
    ok.
