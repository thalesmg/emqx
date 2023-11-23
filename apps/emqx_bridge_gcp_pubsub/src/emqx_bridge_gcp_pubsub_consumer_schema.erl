%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_schema).

-import(hoconsc, [mk/2, ref/2]).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "gcp_pubsub_consumer".

roots() ->
    [].

%%=========================================
%% Action fields
%%=========================================
fields(action) ->
    {gcp_pubsub_consumer,
        mk(
            hoconsc:map(name, ref(?MODULE, consumer_action)),
            #{
                desc => <<"GCP PubSub Consumer Action Config">>,
                required => false
            }
        )};
fields(consumer_action) ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC(consumer_action)
            }
        )
    );
fields(action_parameters) ->
    OldFields = emqx_bridge_gcp_pubsub:fields(consumer),
    DroppedFields = [topic_mapping],
    lists:filter(fun({K, _Sc}) -> not lists:member(K, DroppedFields) end, OldFields) ++
        [
            {mqtt_topic,
                mk(binary(), #{
                    required => true, desc => ?DESC(emqx_bridge_pubsub, consumer_mqtt_topic)
                })},
            {pubsub_topic,
                mk(binary(), #{
                    required => true, desc => ?DESC(emqx_bridge_pubsub, consumer_pubsub_topic)
                })},
            {qos,
                mk(emqx_schema:qos(), #{
                    default => 0, desc => ?DESC(emqx_bridge_pubsub, consumer_mqtt_qos)
                })},
            {payload_template,
                mk(
                    string(),
                    #{
                        default => <<"${.}">>,
                        desc => ?DESC(emqx_bridge_pubsub, consumer_mqtt_payload)
                    }
                )}
        ];
%%=========================================
%% Connector fields
%%=========================================
fields("config_connector") ->
    %% FIXME
    emqx_connector_schema:common_fields() ++
        emqx_bridge_gcp_pubsub:fields(connector_config) ++
        emqx_resource_schema:fields("resource_opts");
%%=========================================
%% HTTP API fields: action
%%=========================================
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2");
fields("post_bridge_v2") ->
    [type_field(), name_field() | fields("put_bridge_v2")];
fields("put_bridge_v2") ->
    fields(consumer_action);
%%=========================================
%% HTTP API fields: connector
%%=========================================
fields("get_connector") ->
    emqx_bridge_schema:status_fields() ++ fields("post_connector");
fields("post_connector") ->
    [type_field(), name_field() | fields("put_connector")];
fields("put_connector") ->
    fields("config_connector").

desc("config_connector") ->
    ?DESC("config_connector");
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(consumer_action) ->
    ?DESC(consumer_action);
desc(_Name) ->
    undefined.

type_field() ->
    {type, mk(gcp_pubsub_consumer, #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"gcp_pubsub_consumer">> => #{
                summary => <<"GCP PubSub Consumer Action">>,
                value => action_example(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"gcp_pubsub_consumer">> => #{
                summary => <<"GCP PubSub Consumer Connector">>,
                value => connector_example(Method)
            }
        }
    ].

conn_bridge_examples(Method) ->
    emqx_bridge_gcp_pubsub:conn_bridge_examples(Method).

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => <<"gcp_pubsub_consumer">>,
            name => <<"my_action">>
        }
    );
action_example(get) ->
    maps:merge(
        action_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
action_example(put) ->
    #{
        enable => true,
        connector => <<"my_connector_name">>,
        description => <<"My action">>,
        local_topic => <<"local/topic">>,
        resource_opts => #{request_timeout => <<"20s">>},
        parameters =>
            #{
                pull_max_messages => 100,
                pubsub_topic => <<"pubsub-topic">>,
                mqtt_topic => <<"mqtt/topic/1">>,
                qos => 1,
                payload_template => <<"${.}">>
            }
    }.

connector_example(get) ->
    maps:merge(
        connector_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => <<"gcp_pubsub_consumer">>,
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        connect_timeout => <<"15s">>,
        resource_opts => #{request_ttl => <<"20s">>},
        service_account_json =>
            #{
                auth_provider_x509_cert_url =>
                    <<"https://www.googleapis.com/oauth2/v1/certs">>,
                auth_uri =>
                    <<"https://accounts.google.com/o/oauth2/auth">>,
                client_email =>
                    <<"test@myproject.iam.gserviceaccount.com">>,
                client_id => <<"123812831923812319190">>,
                client_x509_cert_url =>
                    <<
                        "https://www.googleapis.com/robot/v1/"
                        "metadata/x509/test%40myproject.iam.gserviceaccount.com"
                    >>,
                private_key =>
                    <<
                        "-----BEGIN PRIVATE KEY-----\n"
                        "MIIEvQI..."
                    >>,
                private_key_id => <<"kid">>,
                project_id => <<"myproject">>,
                token_uri =>
                    <<"https://oauth2.googleapis.com/token">>,
                type => <<"service_account">>
            }
    }.
