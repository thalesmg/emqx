%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar).

-include("emqx_bridge_pulsar.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).
%% emqx_ee_bridge "unofficial" API
-export([conn_bridge_examples/1]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_pulsar".

roots() ->
    [].

fields(pulsar_producer) ->
    fields(config) ++ fields(producer_opts);
fields(config) ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {servers,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("servers"),
                    validator => emqx_schema:servers_validator(
                        ?PULSAR_HOST_OPTIONS, _Required = true
                    )
                }
            )},
        {authentication,
            mk(hoconsc:union([none, ref(auth_basic), ref(auth_token)]), #{
                default => none, desc => ?DESC("authentication")
            })}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields(producer_opts) ->
    [
        {batch_size,
            mk(
                pos_integer(),
                #{default => 100, desc => ?DESC("producer_batch_size")}
            )},
        {compression,
            mk(
                hoconsc:enum([no_compression, snappy, zlib]),
                #{default => no_compression, desc => ?DESC("producer_compression")}
            )},
        {send_buffer,
            mk(emqx_schema:bytesize(), #{
                default => <<"1MB">>, desc => ?DESC("producer_send_buffer")
            })},
        {sync_timeout,
            mk(emqx_schema:duration_ms(), #{
                default => <<"3s">>, desc => ?DESC("producer_sync_timeout")
            })},
        {retention_period,
            mk(
                hoconsc:union([infinity, emqx_schema:duration_ms()]),
                #{default => infinity, desc => ?DESC("producer_retention_period")}
            )},
        {max_batch_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"900KB">>, desc => ?DESC("producer_max_batch_bytes")}
            )},
        {local_topic, mk(binary(), #{required => false, desc => ?DESC("producer_local_topic")})},
        {pulsar_topic, mk(binary(), #{required => true, desc => ?DESC("producer_pulsar_topic")})},
        {strategy,
            mk(
                hoconsc:enum([random, roundrobin, first_key_dispatch]),
                #{default => random, desc => ?DESC("producer_strategy")}
            )},
        {buffer, mk(ref(producer_buffer), #{required => false, desc => ?DESC("producer_buffer")})},
        {message,
            mk(ref(producer_pulsar_message), #{
                required => false, desc => ?DESC("producer_message_opts")
            })}
    ];
fields(producer_buffer) ->
    [
        {mode,
            mk(
                hoconsc:enum([memory, disk, hybrid]),
                #{default => memory, desc => ?DESC("buffer_mode")}
            )},
        {per_partition_limit,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"2GB">>, desc => ?DESC("buffer_per_partition_limit")}
            )},
        {segment_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"100MB">>, desc => ?DESC("buffer_segment_bytes")}
            )},
        {memory_overload_protection,
            mk(boolean(), #{
                default => false,
                desc => ?DESC("buffer_memory_overload_protection")
            })}
    ];
fields(producer_pulsar_message) ->
    [
        {key,
            mk(string(), #{default => <<"${.clientid}">>, desc => ?DESC("producer_key_template")})},
        {value, mk(string(), #{default => <<"${.}">>, desc => ?DESC("producer_value_template")})}
    ];
fields(auth_basic) ->
    [
        {username, mk(binary(), #{required => true, desc => ?DESC("auth_basic_username")})},
        {password,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_basic_password"),
                sensitive => true,
                converter => fun emqx_schema:password_converter/2
            })}
    ];
fields(auth_token) ->
    [
        {jwt,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_token_jwt"),
                sensitive => true,
                converter => fun emqx_schema:password_converter/2
            })}
    ];
fields("get_" ++ Type) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Type);
fields("put_" ++ Type) ->
    fields("config_" ++ Type);
fields("post_" ++ Type) ->
    [type_field(), name_field() | fields("config_" ++ Type)];
fields("config_producer") ->
    fields(pulsar_producer).

desc(pulsar_producer) ->
    ?DESC(pulsar_producer_struct);
desc("get_" ++ Type) when Type =:= "producer" ->
    ["Configuration for Pulsar using `GET` method."];
desc("put_" ++ Type) when Type =:= "producer" ->
    ["Configuration for Pulsar using `PUT` method."];
desc("post_" ++ Type) when Type =:= "producer" ->
    ["Configuration for Pulsar using `POST` method."];
desc(_) ->
    undefined.

conn_bridge_examples(_Method) ->
    [
        #{
            <<"pulsar_producer">> => #{
                summary => <<"Pulsar Producer Bridge">>,
                value => #{todo => true}
            }
        }
    ].

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

type_field() ->
    {type, mk(hoconsc:enum([pulsar_producer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
