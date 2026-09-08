%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_grpc_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_gcp_pubsub_consumer_grpc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("grpc/include/grpc.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(service_account_json, service_account_json).
-define(wif_oidc, wif_oidc).
-define(attached_service_account, attached_service_account).

-define(PROXY_NAME, "gcp_emulator_pubsub_no_proxy").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(PREPARED_REQUEST(METHOD, PATH, BODY),
    {prepared_request, {METHOD, PATH, BODY}, #{request_ttl => 1_000}}
).

-define(GRPC_SERVER, gconsu_grpc).
-define(GRPC_SERVER_MOD, emqx_bridge_gcp_pubsub_consumer_test_grpc_server).
-define(GRPC_PORT, 1234).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_gcp_pubsub,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_testcase(TestCase, TCConfig0) ->
    reset_proxy(),
    Path = group_path(TCConfig0, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorName = atom_to_binary(TestCase),
    ServiceAccountJSON =
        #{~"project_id" := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    Authentication =
        case auth_of(TCConfig0) of
            ?service_account_json ->
                #{
                    ~"type" => ~"service_account_json",
                    ~"service_account_json" => emqx_utils_json:encode(ServiceAccountJSON)
                };
            ?wif_oidc ->
                wif_oidc_auth();
            ?attached_service_account ->
                attached_service_account_auth()
        end,
    ConnectorConfig = connector_config(#{
        ~"authentication" => Authentication
    }),
    SourceName = ConnectorName,
    PubSubTopic = Name,
    SourceConfig = source_config(#{
        ~"connector" => ConnectorName,
        ~"parameters" => #{
            ~"topic" => PubSubTopic
        }
    }),
    TCConfig1 = [{project_id, ProjectId} | TCConfig0],
    ensure_topic(ProjectId, PubSubTopic, TCConfig1),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig},
        {pubsub_topic, PubSubTopic}
        | TCConfig1
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    reset_proxy(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        ~"enable" => true,
        ~"description" => ~"my connector",
        ~"tags" => [~"some", ~"tags"],
        ~"url" => ~"http://toxiproxy:8187",
        ~"pool_size" => 4,
        ~"connect_timeout" => ~"5s",
        ~"ssl" => #{~"enable" => false},
        ~"resource_opts" =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, ~"x", InnerConfigMap).

wif_oidc_auth() ->
    #{
        ~"type" => ~"wif",
        ~"gcp_project_id" => ~"myproject",
        ~"gcp_project_number" => ~"123456789012",
        ~"gcp_wif_pool_id" => ~"my-wif",
        ~"gcp_wif_pool_provider_id" => ~"my-wif-provider",
        ~"service_account_email" => ~"sa@myproject.iam.gserviceaccount.com",
        ~"initial_token" => #{
            ~"type" => ~"oidc_client_credentials",
            ~"client_id" => ~"5e870489-067f-4a0d-aa4d-295563d8b2e9",
            ~"client_secret" => ~"super oidc secret",
            ~"endpoint_uri" => ~"https://my.oidc.provider/oauth2/token/uri",
            ~"scope" => ~"api://03e6cfaa-bf6d-4078-b748-cb73834e37f3/.default"
        }
    }.

attached_service_account_auth() ->
    #{
        ~"type" => ~"attached_service_account"
    }.

source_config(Overrides) ->
    Defaults = #{
        ~"enable" => true,
        ~"description" => ~"my action",
        ~"tags" => [~"some", ~"tags"],
        ~"parameters" => #{
            ~"ack_deadline" => ~"10s",
            ~"topic" => ~"please override"
        },
        ~"resource_opts" =>
            emqx_utils_maps:deep_merge(
                emqx_bridge_v2_testlib:common_source_resource_opts(),
                #{~"request_ttl" => ~"1s"}
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, ?SOURCE_TYPE_BIN, ~"x", InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

fmt(Fmt, Ctx) -> emqx_bridge_v2_testlib:fmt(Fmt, Ctx).

fmt_erl(X) ->
    iolist_to_binary(io_lib:format("~p", [X])).

auth_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(
        TCConfig,
        [?service_account_json, ?wif_oidc, ?attached_service_account],
        ?service_account_json
    ).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(TCConfig, Overrides).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

probe_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

with_client(Opts0, _TCConfig, Fn) ->
    Defaults = #{
        host => "toxiproxy",
        port => 8187
    },
    Opts = emqx_utils_maps:deep_merge(Defaults, Opts0),
    #{
        host := Host,
        port := Port
    } = Opts,
    RawServiceAccount = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ClientConfig =
        #{
            connect_timeout => 5_000,
            max_retries => 0,
            pool_size => 1,
            authentication => #{
                type => service_account_json,
                service_account_json => emqx_utils_json:encode(RawServiceAccount)
            },
            jwt_opts => #{aud => ~"https://pubsub.googleapis.com/"},
            transport => tcp,
            host => Host,
            port => Port
        },
    PoolName = ~"control_connector",
    {ok, _ExtraInfo, Client} = emqx_bridge_gcp_pubsub_client:start(PoolName, ClientConfig),
    try
        Fn(Client, Opts)
    after
        ok = emqx_bridge_gcp_pubsub_client:stop(Client)
    end.

ensure_topic(ProjectId, Topic, TCConfig) ->
    with_client(#{}, TCConfig, fun(Client, _Opts) ->
        Method = put,
        Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
        Body = ~"{}",
        on_exit(fun() -> delete_topic(ProjectId, Topic, TCConfig) end),
        Res = emqx_bridge_gcp_pubsub_client:query_sync(
            ?PREPARED_REQUEST(Method, Path, Body),
            Client
        ),
        case Res of
            {ok, _} ->
                ok;
            {error, #{status_code := 409}} ->
                %% already exists
                ok
        end,
        ok
    end).

delete_topic(TCConfig) when is_list(TCConfig) ->
    ProjectId = get_config(project_id, TCConfig),
    Topic = get_config(pubsub_topic, TCConfig),
    delete_topic(ProjectId, Topic, TCConfig).

delete_topic(ProjectId, Topic, TCConfig) ->
    with_client(#{}, TCConfig, fun(Client, _Opts) ->
        Method = delete,
        Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
        Body = ~"",
        Res = emqx_bridge_gcp_pubsub_client:query_sync(
            ?PREPARED_REQUEST(Method, Path, Body),
            Client
        ),
        ct:pal("delete topic ~s\n  res: ~p", [Path, Res]),
        ok
    end).

delete_subscription(TCConfig) when is_list(TCConfig) ->
    SourceName = get_config(source_name, TCConfig),
    Topic = get_config(pubsub_topic, TCConfig),
    ProjectId = get_config(project_id, TCConfig),
    SubId = emqx_bridge_gcp_pubsub_consumer_worker:subscription_id(
        SourceName, Topic, ProjectId
    ),
    delete_subscription(ProjectId, SubId, TCConfig).

delete_subscription(ProjectId, SubId, TCConfig) ->
    with_client(#{}, TCConfig, fun(Client, _Opts) ->
        Method = delete,
        Path = <<"/v1/projects/", ProjectId/binary, "/subscriptions/", SubId/binary>>,
        Body = ~"",
        Res = emqx_bridge_gcp_pubsub_client:query_sync(
            ?PREPARED_REQUEST(Method, Path, Body),
            Client
        ),
        ct:pal("delete subscription ~s\n  res: ~p", [Path, Res]),
        ok
    end).

pubsub_publish(Msgs, Opts0, TCConfig) ->
    DefaultTopic = get_config(pubsub_topic, TCConfig),
    Defaults = #{
        project_id => ~"myproject",
        topic => DefaultTopic
    },
    Opts1 = emqx_utils_maps:deep_merge(Defaults, Opts0),
    with_client(Opts1, TCConfig, fun(Client, Opts) ->
        #{
            project_id := ProjectId,
            topic := Topic
        } = Opts,
        Method = post,
        Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary, ":publish">>,
        Messages =
            lists:map(
                fun(Msg) ->
                    emqx_utils_maps:update_if_present(
                        ~"data",
                        fun
                            (D) when is_binary(D) -> base64:encode(D);
                            (M) when is_map(M) -> base64:encode(emqx_utils_json:encode(M))
                        end,
                        Msg
                    )
                end,
                Msgs
            ),
        Body = emqx_utils_json:encode(#{~"messages" => Messages}),
        {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
            ?PREPARED_REQUEST(Method, Path, Body),
            Client
        ),
        ok
    end).

find_grpc_clients(TCConfig) ->
    #{
        resource_namespace := Namespace,
        type := SourceType,
        name := SourceName
    } = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    {ok, {ConnResId, _SourceResId}} =
        emqx_bridge_v2:get_resource_ids(Namespace, sources, SourceType, SourceName),
    Pool = ?SOURCE_SUP:grpc_client_pool(ConnResId),
    [
        Pid
     || {_Name, Pid} <- gproc_pool:active_workers(Pool)
    ].

kill_grpc_clients(TCConfig) ->
    lists:foreach(
        fun(P) ->
            ct:pal("killing client ~p", [P]),
            exit(P, kill)
        end,
        find_grpc_clients(TCConfig)
    ).

start_mocked_grpc_server(_TestCase, _TCConfig) ->
    on_exit(fun() -> grpc:stop_server(?GRPC_SERVER) end),
    Services = #{
        protos => [emqx_gcp_protos_gen_pubsub_pb],
        services => #{
            'google.pubsub.v1.Subscriber' => ?GRPC_SERVER_MOD
        }
    },
    Opts = [{ranch_opts, #{shutdown => brutal_kill}}],
    {ok, _} = grpc:start_server(?GRPC_SERVER, ?GRPC_PORT, Services, Opts),
    {ok, Agent} = emqx_utils_agent:start_link(#{}),
    ?GRPC_SERVER_MOD:set_agent(Agent),
    URL = fmt(~"http://127.0.0.1:${p}", #{p => ?GRPC_PORT}),
    {URL, Agent}.

stop_mocked_grpc_server() ->
    grpc:stop_server(?GRPC_SERVER).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "gcp_pubsub_consumer_grpc_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_consume(TCConfig) ->
    ProjectId = get_config(project_id, TCConfig),
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    Payload = #{~"key" => ~"value"},
    Attributes = #{~"hkey" => ~"hval"},
    ProduceFn = fun() ->
        pubsub_publish(
            [
                #{
                    ~"data" => Payload,
                    ~"orderingKey" => ~"ok",
                    ~"attributes" => Attributes
                }
            ],
            #{},
            TCConfig
        )
    end,
    Encoded = emqx_utils_json:encode(Payload),
    TopicResource = emqx_bridge_gcp_pubsub_consumer_worker:topic_resource(
        ProjectId,
        PubSubTopic
    ),
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                attributes := Attributes,
                message_id := _,
                ordering_key := ~"ok",
                publish_time := #{seconds := _, nanos := _},
                topic := TopicResource,
                value := Encoded
            },
            Message
        )
    end,
    ok = emqx_bridge_v2_testlib:t_consume(
        TCConfig,
        #{
            test_timeout => 15_000,
            consumer_ready_tracepoint => ?match_event(
                #{?snk_kind := "gcp_pubsub_consumer_grpc_worker_pulling"}
            ),
            consumer_ready_timeout => 10_000,
            produce_fn => ProduceFn,
            check_fn => CheckFn,
            produce_tracepoint => ?match_event(
                #{
                    ?snk_kind := "gcp_pubsub_consumer_grpc_worker_process_msg",
                    ?snk_span := {complete, _}
                }
            )
        }
    ),
    ok.

-doc """
Verifies that we can override the project id from authentication by specifying a fully
qualified topic resource as the topic.
""".
t_consume_cross_project_topic(TCConfig) when is_list(TCConfig) ->
    OtherProjectId = ~"cross-project",
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    Payload = #{~"key" => ~"value"},
    Attributes = #{~"hkey" => ~"hval"},
    OtherTopicResource = emqx_bridge_gcp_pubsub_consumer_worker:topic_resource(
        OtherProjectId,
        PubSubTopic
    ),
    ensure_topic(OtherProjectId, PubSubTopic, TCConfig),
    ProduceFn = fun() ->
        pubsub_publish(
            [
                #{
                    ~"data" => Payload,
                    ~"orderingKey" => ~"ok",
                    ~"attributes" => Attributes
                }
            ],
            #{
                project_id => OtherProjectId
            },
            TCConfig
        )
    end,
    Encoded = emqx_utils_json:encode(Payload),
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                attributes := Attributes,
                message_id := _,
                ordering_key := ~"ok",
                publish_time := #{seconds := _, nanos := _},
                topic := OtherTopicResource,
                value := Encoded
            },
            Message
        )
    end,
    SourceOverrides = #{~"parameters" => #{~"topic" => OtherTopicResource}},
    ok = emqx_bridge_v2_testlib:t_consume(
        TCConfig,
        #{
            test_timeout => 15_000,
            source_overrides => SourceOverrides,
            consumer_ready_tracepoint => ?match_event(
                #{?snk_kind := "gcp_pubsub_consumer_grpc_worker_pulling"}
            ),
            consumer_ready_timeout => 10_000,
            produce_fn => ProduceFn,
            check_fn => CheckFn,
            produce_tracepoint => ?match_event(
                #{
                    ?snk_kind := "gcp_pubsub_consumer_grpc_worker_process_msg",
                    ?snk_span := {complete, _}
                }
            )
        }
    ),
    ok.

t_nonexistent_topic(TCConfig) when is_list(TCConfig) ->
    {201, #{~"status" := ~"connected"}} = create_connector_api(TCConfig, #{}),
    NonexistentTopic = ~"xxxx",
    ExpectedReason = fmt_erl(
        {unhealthy_target, ~"Topic not found: Subscription topic does not exist"}
    ),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason
        }},
        create_source_api(TCConfig, #{
            ~"parameters" => #{~"topic" => NonexistentTopic}
        })
    ),
    ok.

-doc """
Verifies that the source is marked as unhealthy if the topic (and subscription) go away
after it's alreaddy running.

Curiously, gcp pubsub doesn't seem to return any errors from the subscription if the topic
is deleted while the subscription still exists...  so we also delete the subscription so
the worker notices it.
""".
t_subscription_deleted_while_consumer_is_running(TCConfig) when is_list(TCConfig) ->
    {201, #{~"status" := ~"connected"}} = create_connector_api(TCConfig, #{}),
    {201, #{~"status" := ~"connected"}} = create_source_api(TCConfig, #{}),
    delete_topic(TCConfig),
    delete_subscription(TCConfig),
    ExpectedReason = fmt_erl(
        {unhealthy_target, ~"Topic not found: Subscription topic does not exist"}
    ),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                ~"status" := ~"disconnected",
                ~"status_reason" := ExpectedReason
            }},
            get_source_api(TCConfig)
        )
    ),
    ok.

-doc """
Checks that doing a dry-run does not disturb a running source.
""".
t_probe_does_not_disturb_running_source(TCConfig) when is_list(TCConfig) ->
    {201, #{~"status" := ~"connected"}} = create_connector_api(TCConfig, #{}),
    {201, #{~"status" := ~"connected"}} = create_source_api(TCConfig, #{}),
    ?assertMatch({204, _}, probe_source_api(TCConfig, #{})),
    %% force health check now
    ?assertMatch(
        #{status := ?status_connected},
        emqx_bridge_v2_testlib:health_check_channel(TCConfig)
    ),
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        get_source_api(TCConfig)
    ),
    ok.

t_permission_denied_when_updating_sub_then_fixed(TCConfig) when is_list(TCConfig) ->
    {URL, _Agent} = start_mocked_grpc_server(?FUNCTION_NAME, TCConfig),
    {201, #{~"status" := ~"connected"}} = create_connector_api(TCConfig, #{
        ~"url" => URL
    }),
    ?GRPC_SERVER_MOD:agent_update(fun(St) ->
        St#{update_subscription => [{reply_error, ?GRPC_STATUS_PERMISSION_DENIED}]}
    end),
    ExpectedReason = fmt_erl({unhealthy_target, ~"Permission denied"}),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason
        }},
        create_source_api(TCConfig, #{})
    ),
    %% now we "fix" the permission issue; should recover by itself.
    ct:pal("restoring permission"),
    ?GRPC_SERVER_MOD:agent_update(fun(St) ->
        St#{update_subscription => [default]}
    end),
    ?retry(
        1_000,
        20,
        ?assertMatch(
            {200, #{~"status" := ~"connected"}},
            get_source_api(TCConfig)
        )
    ),
    ok.

t_permission_denied_when_creating_sub_then_fixed(TCConfig) when is_list(TCConfig) ->
    {URL, _Agent} = start_mocked_grpc_server(?FUNCTION_NAME, TCConfig),
    {201, #{~"status" := ~"connected"}} = create_connector_api(TCConfig, #{
        ~"url" => URL
    }),
    ?GRPC_SERVER_MOD:agent_update(fun(St) ->
        St#{
            update_subscription => [{reply_error, ?GRPC_STATUS_NOT_FOUND}],
            create_subscription => [{reply_error, ?GRPC_STATUS_PERMISSION_DENIED}]
        }
    end),
    ExpectedReason = fmt_erl({unhealthy_target, ~"Permission denied"}),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason
        }},
        create_source_api(TCConfig, #{})
    ),
    %% now we "fix" the permission issue; should recover by itself.
    ct:pal("restoring permission"),
    ?GRPC_SERVER_MOD:agent_update(fun(St) ->
        St#{create_subscription => [default]}
    end),
    ?retry(
        1_000,
        20,
        ?assertMatch(
            {200, #{~"status" := ~"connected"}},
            get_source_api(TCConfig)
        )
    ),
    ok.
