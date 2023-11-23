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
-module(emqx_connector_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([
    transform_bridges_v1_to_connectors_and_bridges_v2/1,
    transform_bridge_v1_config_to_action_config/4,
    top_level_common_connector_keys/0
]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([get_response/0, put_request/0, post_request/0]).

-export([connector_type_to_bridge_types/1]).
-export([common_fields/0]).

-export([resource_opts_fields/0, resource_opts_fields/1]).

-if(?EMQX_RELEASE_EDITION == ee).
enterprise_api_schemas(Method) ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    _ = emqx_connector_ee_schema:module_info(),
    case erlang:function_exported(emqx_connector_ee_schema, api_schemas, 1) of
        true -> emqx_connector_ee_schema:api_schemas(Method);
        false -> []
    end.

enterprise_fields_connectors() ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    _ = emqx_connector_ee_schema:module_info(),
    case erlang:function_exported(emqx_connector_ee_schema, fields, 1) of
        true ->
            emqx_connector_ee_schema:fields(connectors);
        false ->
            []
    end.

-else.

enterprise_api_schemas(_Method) -> [].

enterprise_fields_connectors() -> [].

-endif.

connector_type_to_bridge_types(azure_event_hub_producer) -> [azure_event_hub_producer];
connector_type_to_bridge_types(confluent_producer) -> [confluent_producer];
connector_type_to_bridge_types(gcp_pubsub_consumer) -> [gcp_pubsub_consumer];
connector_type_to_bridge_types(gcp_pubsub_producer) -> [gcp_pubsub_producer];
connector_type_to_bridge_types(kafka_producer) -> [kafka, kafka_producer];
connector_type_to_bridge_types(syskeeper_forwarder) -> [syskeeper_forwarder];
connector_type_to_bridge_types(syskeeper_proxy) -> [].

actions_config_name() -> <<"actions">>.

has_connector_field(BridgeConf, ConnectorFields) ->
    lists:any(
        fun({ConnectorFieldName, _Spec}) ->
            maps:is_key(to_bin(ConnectorFieldName), BridgeConf)
        end,
        ConnectorFields
    ).

bridge_configs_to_transform(
    _BridgeType, [] = _BridgeNameBridgeConfList, _ConnectorFields, _RawConfig
) ->
    [];
bridge_configs_to_transform(
    BridgeType, [{BridgeName, BridgeConf} | Rest], ConnectorFields, RawConfig
) ->
    case has_connector_field(BridgeConf, ConnectorFields) of
        true ->
            PreviousRawConfig =
                emqx_utils_maps:deep_get(
                    [<<"actions">>, to_bin(BridgeType), to_bin(BridgeName)],
                    RawConfig,
                    undefined
                ),
            [
                {BridgeType, BridgeName, BridgeConf, ConnectorFields, PreviousRawConfig}
                | bridge_configs_to_transform(BridgeType, Rest, ConnectorFields, RawConfig)
            ];
        false ->
            bridge_configs_to_transform(BridgeType, Rest, ConnectorFields, RawConfig)
    end.

split_bridge_to_connector_and_actions(
    {ConnectorsMap, {BridgeType, BridgeName, BridgeV1Conf, ConnectorFields, PreviousRawConfig}}
) ->
    ConnectorMap =
        case emqx_action_info:has_custom_bridge_v1_config_to_connector_config(BridgeType) of
            true ->
                emqx_action_info:bridge_v1_config_to_connector_config(
                    BridgeType, BridgeV1Conf
                );
            false ->
                %% We do an automatic transfomation to get the connector config
                %% if the callback is not defined.
                %% Get connector fields from bridge config
                lists:foldl(
                    fun({ConnectorFieldName, _Spec}, ToTransformSoFar) ->
                        case maps:is_key(to_bin(ConnectorFieldName), BridgeV1Conf) of
                            true ->
                                NewToTransform = maps:put(
                                    to_bin(ConnectorFieldName),
                                    maps:get(to_bin(ConnectorFieldName), BridgeV1Conf),
                                    ToTransformSoFar
                                ),
                                NewToTransform;
                            false ->
                                ToTransformSoFar
                        end
                    end,
                    #{},
                    ConnectorFields
                )
        end,
    %% Generate a connector name, if needed.  Avoid doing so if there was a previous config.
    ConnectorName =
        case PreviousRawConfig of
            #{<<"connector">> := ConnectorName0} -> ConnectorName0;
            _ -> generate_connector_name(ConnectorsMap, BridgeName, 0)
        end,
    ActionConfigs =
        case emqx_action_info:has_custom_bridge_v1_config_to_action_config(BridgeType) of
            true ->
                emqx_action_info:bridge_v1_config_to_action_config(
                    BridgeType, BridgeV1Conf, ConnectorName
                );
            false ->
                transform_bridge_v1_config_to_action_config(
                    BridgeV1Conf, ConnectorName, ConnectorFields
                )
        end,
    case is_list(ActionConfigs) of
        true ->
            [
                {BridgeType, BridgeName, ActionConfig, ConnectorName, ConnectorMap}
             || ActionConfig <- ActionConfigs
            ];
        false ->
            [{BridgeType, BridgeName, ActionConfigs, ConnectorName, ConnectorMap}]
    end.

transform_bridge_v1_config_to_action_config(
    BridgeV1Conf, ConnectorName, ConnectorConfSchemaMod, ConnectorConfSchemaName
) ->
    ConnectorFields = ConnectorConfSchemaMod:fields(ConnectorConfSchemaName),
    transform_bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName, ConnectorFields
    ).

top_level_common_connector_keys() ->
    [
        <<"enable">>,
        <<"connector">>,
        <<"local_topic">>,
        <<"resource_opts">>,
        <<"description">>,
        <<"parameters">>
    ].

transform_bridge_v1_config_to_action_config(
    BridgeV1Conf, ConnectorName, ConnectorFields
) ->
    TopKeys = top_level_common_connector_keys(),
    TopKeysMap = maps:from_keys(TopKeys, true),
    %% Remove connector fields
    ActionMap0 = lists:foldl(
        fun
            ({enable, _Spec}, ToTransformSoFar) ->
                %% Enable filed is used in both
                ToTransformSoFar;
            ({ConnectorFieldName, _Spec}, ToTransformSoFar) ->
                ConnectorFieldNameBin = to_bin(ConnectorFieldName),
                case
                    maps:is_key(ConnectorFieldNameBin, BridgeV1Conf) andalso
                        (not maps:is_key(ConnectorFieldNameBin, TopKeysMap))
                of
                    true ->
                        maps:remove(ConnectorFieldNameBin, ToTransformSoFar);
                    false ->
                        ToTransformSoFar
                end
        end,
        BridgeV1Conf,
        ConnectorFields
    ),
    %% Add the connector field
    ActionMap1 = maps:put(<<"connector">>, ConnectorName, ActionMap0),
    TopMap = maps:with(TopKeys, ActionMap1),
    RestMap = maps:without(TopKeys, ActionMap1),
    %% Other parameters should be stuffed into `parameters'
    emqx_utils_maps:deep_merge(TopMap, #{<<"parameters">> => RestMap}).

generate_connector_name(ConnectorsMap, BridgeName, Attempt) ->
    ConnectorNameList =
        case Attempt of
            0 ->
                io_lib:format("connector_~s", [BridgeName]);
            _ ->
                io_lib:format("connector_~s_~p", [BridgeName, Attempt + 1])
        end,
    ConnectorName = iolist_to_binary(ConnectorNameList),
    case maps:is_key(ConnectorName, ConnectorsMap) of
        true ->
            generate_connector_name(ConnectorsMap, BridgeName, Attempt + 1);
        false ->
            ConnectorName
    end.

transform_old_style_bridges_to_connector_and_actions_of_type(
    {ConnectorType, #{type := ?MAP(_Name, ?R_REF(ConnectorConfSchemaMod, ConnectorConfSchemaName))}},
    RawConfig
) ->
    ConnectorFields = ConnectorConfSchemaMod:fields(ConnectorConfSchemaName),
    BridgeTypes = ?MODULE:connector_type_to_bridge_types(ConnectorType),
    BridgesConfMap = maps:get(<<"bridges">>, RawConfig, #{}),
    ConnectorsConfMap = maps:get(<<"connectors">>, RawConfig, #{}),
    BridgeConfigsToTransform =
        lists:flatmap(
            fun(BridgeType) ->
                BridgeNameToBridgeMap = maps:get(to_bin(BridgeType), BridgesConfMap, #{}),
                BridgeNameBridgeConfList = maps:to_list(BridgeNameToBridgeMap),
                bridge_configs_to_transform(
                    BridgeType, BridgeNameBridgeConfList, ConnectorFields, RawConfig
                )
            end,
            BridgeTypes
        ),
    ConnectorsWithTypeMap = maps:get(to_bin(ConnectorType), ConnectorsConfMap, #{}),
    BridgeConfigsToTransformWithConnectorConf = lists:zip(
        lists:duplicate(length(BridgeConfigsToTransform), ConnectorsWithTypeMap),
        BridgeConfigsToTransform
    ),
    ActionConnectorTuples = lists:flatmap(
        fun split_bridge_to_connector_and_actions/1,
        BridgeConfigsToTransformWithConnectorConf
    ),
    %% Add connectors and actions and remove bridges
    lists:foldl(
        fun({BridgeType, BridgeName, ActionMap, ConnectorName, ConnectorMap}, RawConfigSoFar) ->
            %% Add connector
            RawConfigSoFar1 = emqx_utils_maps:deep_put(
                [<<"connectors">>, to_bin(ConnectorType), ConnectorName],
                RawConfigSoFar,
                ConnectorMap
            ),
            %% Remove bridge (v1)
            RawConfigSoFar2 = emqx_utils_maps:deep_remove(
                [<<"bridges">>, to_bin(BridgeType), BridgeName],
                RawConfigSoFar1
            ),
            %% Add action
            RawConfigSoFar3 = emqx_utils_maps:deep_put(
                [actions_config_name(), to_bin(maybe_rename(BridgeType)), BridgeName],
                RawConfigSoFar2,
                ActionMap
            ),
            RawConfigSoFar3
        end,
        RawConfig,
        ActionConnectorTuples
    ).

transform_bridges_v1_to_connectors_and_bridges_v2(RawConfig) ->
    ConnectorFields = ?MODULE:fields(connectors),
    lists:foldl(
        fun transform_old_style_bridges_to_connector_and_actions_of_type/2,
        RawConfig,
        ConnectorFields
    ).

%% v1 uses 'kafka' as bridge type v2 uses 'kafka_producer'
maybe_rename(kafka) ->
    kafka_producer;
maybe_rename(Name) ->
    Name.

%%======================================================================================
%% HOCON Schema Callbacks
%%======================================================================================

%% For HTTP APIs
get_response() ->
    api_schema("get").

put_request() ->
    api_schema("put").

post_request() ->
    api_schema("post").

api_schema(Method) ->
    EE = enterprise_api_schemas(Method),
    hoconsc:union(connector_api_union(EE)).

connector_api_union(Refs) ->
    Index = maps:from_list(Refs),
    fun
        (all_union_members) ->
            maps:values(Index);
        ({value, V}) ->
            case V of
                #{<<"type">> := T} ->
                    case maps:get(T, Index, undefined) of
                        undefined ->
                            throw(#{
                                field_name => type,
                                value => T,
                                reason => <<"unknown connector type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    maps:values(Index)
            end
    end.

%% general config
namespace() -> "connector".

tags() ->
    [<<"Connector">>].

-dialyzer({nowarn_function, roots/0}).

roots() ->
    case fields(connectors) of
        [] ->
            [
                {connectors,
                    ?HOCON(hoconsc:map(name, typerefl:map()), #{importance => ?IMPORTANCE_LOW})}
            ];
        _ ->
            [{connectors, ?HOCON(?R_REF(connectors), #{importance => ?IMPORTANCE_LOW})}]
    end.

fields(connectors) ->
    [] ++ enterprise_fields_connectors().

desc(connectors) ->
    ?DESC("desc_connectors");
desc(_) ->
    undefined.

common_fields() ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {description, emqx_schema:description_schema()}
    ].

resource_opts_fields() ->
    resource_opts_fields(_Overrides = []).

resource_opts_fields(Overrides) ->
    %% Note: these don't include buffer-related configurations because buffer workers are
    %% tied to the action.
    ConnectorROFields = [
        health_check_interval,
        query_mode,
        request_ttl,
        start_after_created,
        start_timeout
    ],
    lists:filter(
        fun({Key, _Sc}) -> lists:member(Key, ConnectorROFields) end,
        emqx_resource_schema:create_opts(Overrides)
    ).

%%======================================================================================
%% Helper Functions
%%======================================================================================

to_bin(Atom) when is_atom(Atom) ->
    list_to_binary(atom_to_list(Atom));
to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Something) ->
    Something.

-ifdef(TEST).
-include_lib("hocon/include/hocon_types.hrl").
schema_homogeneous_test() ->
    case
        lists:filtermap(
            fun({_Name, Schema}) ->
                is_bad_schema(Schema)
            end,
            fields(connectors)
        )
    of
        [] ->
            ok;
        List ->
            throw(List)
    end.

is_bad_schema(#{type := ?MAP(_, ?R_REF(Module, TypeName))}) ->
    Fields = Module:fields(TypeName),
    ExpectedFieldNames = common_field_names(),
    MissingFileds = lists:filter(
        fun(Name) -> lists:keyfind(Name, 1, Fields) =:= false end, ExpectedFieldNames
    ),
    case MissingFileds of
        [] ->
            false;
        _ ->
            {true, #{
                schema_modle => Module,
                type_name => TypeName,
                missing_fields => MissingFileds
            }}
    end.

common_field_names() ->
    [
        enable, description
    ].

-endif.
