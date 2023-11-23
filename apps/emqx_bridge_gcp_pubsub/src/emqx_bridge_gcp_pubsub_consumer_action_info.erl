%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_action_config/2
]).

bridge_v1_type_name() -> gcp_pubsub_consumer.

action_type_name() -> gcp_pubsub_consumer.

connector_type_name() -> gcp_pubsub_consumer.

schema_module() -> emqx_bridge_gcp_pubsub_consumer_schema.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    CommonActionKeys = emqx_bridge_v2_schema:top_level_common_action_keys(),
    Config1 = emqx_utils_maps:rename(<<"consumer">>, <<"parameters">>, BridgeV1Config),
    Config2 = maps:with(CommonActionKeys, Config1),
    TopicMappings = emqx_utils_maps:deep_get([<<"parameters">>, <<"topic_mapping">>], Config2, []),
    Config3 = emqx_utils_maps:deep_remove([<<"parameters">>, <<"topic_mapping">>], Config2),
    lists:map(
        fun(TopicMapping) ->
            NewParams = maps:with(
                [
                    <<"mqtt_topic">>,
                    <<"pubsub_topic">>,
                    <<"payload_template">>,
                    <<"qos">>
                ],
                TopicMapping
            ),
            emqx_utils_maps:deep_merge(
                Config3,
                #{
                    <<"connector">> => ConnectorName,
                    <<"parameters">> => NewParams
                }
            )
        end,
        TopicMappings
    ).
