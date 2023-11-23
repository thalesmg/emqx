%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_impl_consumer).

-behaviour(emqx_resource).

%% `emqx_resource' API
-export([
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

%% health check API
-export([
    mark_as_unhealthy/2,
    clear_unhealthy/1,
    check_if_unhealthy/1
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-type mqtt_config() :: #{
    mqtt_topic := emqx_types:topic(),
    qos := emqx_types:qos(),
    payload_template := emqx_placeholder:tmpl_token()
}.
-type connector_config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    pool_size := non_neg_integer(),
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    service_account_json := emqx_bridge_gcp_pubsub_client:service_account_json(),
    any() => term()
}.
-type action_config() :: #{
    bridge_name := binary(),
    parameters := #{
        consumer_workers_per_topic := pos_integer(),
        mqtt_topic := emqx_types:topic(),
        payload_template := binary(),
        pubsub_topic := binary(),
        qos := emqx_types:qos()
    },
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    any() => term()
}.
-type connector_state() :: #{
    client := emqx_bridge_gcp_pubsub_client:state(),
    installed_actions := #{action_resource_id() => action_state()},
    service_account_json := emqx_bridge_gcp_pubsub_client:service_account_json()
}.
-type action_state() :: #{
    client := emqx_bridge_gcp_pubsub_client:state(),
    pool_name := action_resource_id()
}.

-export_type([mqtt_config/0]).

-define(AUTO_RECONNECT_S, 2).
-define(DEFAULT_FORGET_INTERVAL, timer:seconds(60)).
-define(OPTVAR_UNHEALTHY(INSTANCE_ID), {?MODULE, topic_not_found, INSTANCE_ID}).
-define(TOPIC_MESSAGE,
    "GCP PubSub topics are invalid.  Please check the logs, check if the "
    "topics exist in GCP and if the service account has permissions to use them."
).
-define(PERMISSION_MESSAGE,
    "Permission denied while verifying topic existence.  Please check that the "
    "provided service account has the correct permissions configured."
).

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------

-spec callback_mode() -> callback_mode().
callback_mode() -> async_if_possible.

-spec query_mode(any()) -> query_mode().
query_mode(_Config) -> no_queries.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, term()}.
on_start(InstanceId, Config0) ->
    %% ensure it's a binary key map
    Config = maps:update_with(service_account_json, fun emqx_utils_maps:binary_key_map/1, Config0),
    case emqx_bridge_gcp_pubsub_client:start(InstanceId, Config) of
        {ok, Client} ->
            ServiceAccountJSON = maps:get(service_account_json, Config),
            State = #{
                client => Client,
                installed_actions => #{},
                service_account_json => ServiceAccountJSON
            },
            ?tp(gcp_pubsub_consumer_connector_started, #{}),
            {ok, State};
        Error ->
            Error
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok | {error, term()}.
on_stop(ConnectorResId, ConnectorState) ->
    ?tp(gcp_pubsub_consumer_stop_enter, #{}),
    clear_unhealthy(ConnectorResId),
    case ConnectorState of
        #{installed_actions := InstalledActions} ->
            maps:foreach(
                fun(ActionResId, _ActionState) -> stop_consumers(ActionResId) end,
                InstalledActions
            );
        _ ->
            ok
    end,
    emqx_bridge_gcp_pubsub_client:stop(ConnectorResId).

-spec on_get_status(connector_resource_id(), connector_state()) ->
    connected | connecting | {disconnected, connector_state(), _}.
on_get_status(_ConnectorResId, #{client := Client} = _ConnectorState) ->
    get_client_status(Client);
on_get_status(_ConnectorResId, _ConnectorState) ->
    ?status_disconnected.

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnectorResId, ConnectorState0, ActionResId, ActionConfig) ->
    #{installed_actions := InstalledActions0} = ConnectorState0,
    case install_channel(ActionResId, ActionConfig, ConnectorState0) of
        {ok, ChannelState} ->
            InstalledActions = InstalledActions0#{ActionResId => ChannelState},
            ConnectorState = ConnectorState0#{installed_actions := InstalledActions},
            {ok, ConnectorState};
        Error ->
            Error
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(_ConnectorResId, ConnectorState0, ActionResId) ->
    #{installed_actions := InstalledActions0} = ConnectorState0,
    stop_consumers(ActionResId),
    InstalledActions = maps:remove(ActionResId, InstalledActions0),
    ConnectorState = ConnectorState0#{installed_actions := InstalledActions},
    {ok, ConnectorState}.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), action_config()}].
on_get_channels(ConnectorResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnectorResId).

-spec on_get_channel_status(connector_resource_id(), action_resource_id(), connector_state()) ->
    health_check_status().
on_get_channel_status(_ConnectorResId, ActionResId, ConnectorState) ->
    case check_if_unhealthy(ActionResId) of
        {error, topic_not_found} ->
            {disconnected, {unhealthy_target, ?TOPIC_MESSAGE}};
        {error, permission_denied} ->
            {disconnected, {unhealthy_target, ?PERMISSION_MESSAGE}};
        {error, bad_credentials} ->
            {disconnected, {unhealthy_target, ?PERMISSION_MESSAGE}};
        ok ->
            #{client := Client} = ConnectorState,
            check_workers(ActionResId, Client)
    end.

%%-------------------------------------------------------------------------------------------------
%% Health check API (signalled by consumer worker)
%%-------------------------------------------------------------------------------------------------

-spec mark_as_unhealthy(
    connector_resource_id(),
    topic_not_found
    | permission_denied
    | bad_credentials
) -> ok.
mark_as_unhealthy(InstanceId, Reason) ->
    optvar:set(?OPTVAR_UNHEALTHY(InstanceId), Reason),
    ok.

-spec clear_unhealthy(connector_resource_id()) -> ok.
clear_unhealthy(InstanceId) ->
    optvar:unset(?OPTVAR_UNHEALTHY(InstanceId)),
    ?tp(gcp_pubsub_consumer_clear_unhealthy, #{}),
    ok.

-spec check_if_unhealthy(action_resource_id()) ->
    ok
    | {error,
        topic_not_found
        | permission_denied
        | bad_credentials}.
check_if_unhealthy(ActionResId) ->
    case optvar:peek(?OPTVAR_UNHEALTHY(ActionResId)) of
        {ok, Reason} ->
            {error, Reason};
        undefined ->
            ok
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

install_channel(ActionResId, ActionConfig, ConnectorState) ->
    #{
        client := Client,
        service_account_json := #{<<"project_id">> := ProjectId}
    } = ConnectorState,
    #{
        bridge_name := BridgeName,
        parameters := ConsumerConfig0,
        resource_opts := #{request_ttl := RequestTTL}
    } = ActionConfig,
    Hookpoint = emqx_bridge_resource:bridge_hookpoint(ActionResId),
    ConsumerConfig1 = maps:update_with(
        payload_template, fun emqx_placeholder:preproc_tmpl/1, ConsumerConfig0
    ),
    #{
        consumer_workers_per_topic := ConsumerWorkersPerTopic,
        mqtt_topic := MQTTTopic,
        payload_template := PayloadTemplate,
        pubsub_topic := PubSubTopic,
        qos := QoS
    } = ConsumerConfig1,
    MQTTConfig = #{
        mqtt_topic => MQTTTopic,
        payload_template => PayloadTemplate,
        qos => QoS
    },
    ConsumerConfig = ConsumerConfig1#{
        auto_reconnect => ?AUTO_RECONNECT_S,
        bridge_name => BridgeName,
        client => Client,
        forget_interval => forget_interval(RequestTTL),
        hookpoint => Hookpoint,
        instance_id => ActionResId,
        pool_size => ConsumerWorkersPerTopic,
        project_id => ProjectId,
        pubsub_topic => PubSubTopic,
        pull_retry_interval => RequestTTL,
        mqtt_config => MQTTConfig
    },
    ConsumerOpts = maps:to_list(ConsumerConfig),
    %% FIXME
    case do_validate_pubsub_topics(Client, [PubSubTopic]) of
        ok ->
            ok;
        {error, not_found} ->
            throw(
                {unhealthy_target, ?TOPIC_MESSAGE}
            );
        {error, permission_denied} ->
            throw(
                {unhealthy_target, ?PERMISSION_MESSAGE}
            );
        {error, bad_credentials} ->
            throw(
                {unhealthy_target, ?PERMISSION_MESSAGE}
            );
        {error, _} ->
            %% connection might be down; we'll have to check topic existence during health
            %% check, or the workers will kill themselves when they realized there's no
            %% topic when upserting their subscription.
            ok
    end,
    case
        emqx_resource_pool:start(ActionResId, emqx_bridge_gcp_pubsub_consumer_worker, ConsumerOpts)
    of
        ok ->
            %% TODO: do we still need this?
            State = #{
                client => Client,
                pool_name => ActionResId
            },
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

stop_consumers(ActionResId) ->
    _ = log_when_error(
        fun() ->
            ok = emqx_resource_pool:stop(ActionResId)
        end,
        #{
            msg => "failed_to_stop_pull_worker_pool",
            instance_id => ActionResId
        }
    ),
    ok.

convert_topic_mapping(TopicMappingList) ->
    lists:foldl(
        fun(Fields, Acc) ->
            #{
                pubsub_topic := PubSubTopic,
                mqtt_topic := MQTTTopic,
                qos := QoS,
                payload_template := PayloadTemplate0
            } = Fields,
            PayloadTemplate = emqx_placeholder:preproc_tmpl(PayloadTemplate0),
            Acc#{
                PubSubTopic => #{
                    payload_template => PayloadTemplate,
                    mqtt_topic => MQTTTopic,
                    qos => QoS
                }
            }
        end,
        #{},
        TopicMappingList
    ).

validate_pubsub_topics(TopicMapping, Client) ->
    PubSubTopics = maps:keys(TopicMapping),
    do_validate_pubsub_topics(Client, PubSubTopics).

do_validate_pubsub_topics(Client, [Topic | Rest]) ->
    case check_for_topic_existence(Topic, Client) of
        ok ->
            do_validate_pubsub_topics(Client, Rest);
        {error, _} = Err ->
            Err
    end;
do_validate_pubsub_topics(_Client, []) ->
    %% we already validate that the mapping is not empty in the config schema.
    ok.

check_for_topic_existence(Topic, Client) ->
    Res = emqx_bridge_gcp_pubsub_client:get_topic(Topic, Client),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 404}} ->
            {error, not_found};
        {error, #{status_code := 403}} ->
            {error, permission_denied};
        {error, #{status_code := 401}} ->
            {error, bad_credentials};
        {error, Reason} ->
            ?tp(warning, "gcp_pubsub_consumer_check_topic_error", #{reason => Reason}),
            {error, Reason}
    end.

-spec get_client_status(emqx_bridge_gcp_pubsub_client:state()) -> connected | connecting.
get_client_status(Client) ->
    case emqx_bridge_gcp_pubsub_client:get_status(Client) of
        disconnected -> connecting;
        connected -> connected
    end.

-spec check_workers(connector_resource_id(), emqx_bridge_gcp_pubsub_client:state()) ->
    connected | connecting.
check_workers(ActionResId, Client) ->
    case
        emqx_resource_pool:health_check_workers(
            ActionResId,
            fun emqx_bridge_gcp_pubsub_consumer_worker:health_check/1,
            emqx_resource_pool:health_check_timeout(),
            #{return_values => true}
        )
    of
        {ok, []} ->
            connecting;
        {ok, Values} ->
            AllOk = lists:all(fun(S) -> S =:= subscription_ok end, Values),
            case AllOk of
                true ->
                    get_client_status(Client);
                false ->
                    connecting
            end;
        {error, _} ->
            connecting
    end.

log_when_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.

forget_interval(infinity) -> ?DEFAULT_FORGET_INTERVAL;
forget_interval(Timeout) -> 2 * Timeout.
