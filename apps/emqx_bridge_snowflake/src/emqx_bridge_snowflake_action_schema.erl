%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_snowflake_action_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_snowflake.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    bridge_v2_examples/1
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "action_snowflake".

roots() ->
    [].

fields(Field) when
    Field == "get_bridge_v2";
    Field == "put_bridge_v2";
    Field == "post_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(?ACTION_TYPE));
fields(action) ->
    {?ACTION_TYPE,
        mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION_TYPE)),
            #{
                desc => <<"Snowflake Action Config">>,
                required => false
            }
        )};
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            mkunion(mode, #{
                <<"direct">> => ref(direct_parameters),
                <<"aggregated">> => ref(aggreg_parameters)
            }),
            #{
                required => true,
                desc => ?DESC("parameters")
            }
        ),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(aggreg_parameters) ->
    [ {mode, mk(aggregated, #{required => true, desc => ?DESC("aggregated_mode")})}
    , {aggregation, mk(ref(aggregation), #{required => true, desc => ?DESC("aggregation")})}
    , {private_key, emqx_schema_secret:mk(#{required => true, desc => ?DESC("private_key")})}
    , {database, mk(binary(), #{required => true, desc => ?DESC("database")})}
    , {schema, mk(binary(), #{required => true, desc => ?DESC("schema")})}
    %% , {table, mk(binary(), #{required => true, desc => ?DESC("table")})}
    , {pipe, mk(binary(), #{required => true, desc => ?DESC("pipe")})}
    , {pipe_user, mk(binary(), #{required => true, desc => ?DESC("pipe_user")})}
    , {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"15s">>, desc => ?DESC("connect_timeout")
            })}
    , {pipelining, mk(pos_integer(), #{default => 100, desc => ?DESC("pipelining")})}
    , {pool_size, mk(pos_integer(), #{default => 8, desc => ?DESC("pool_size")})}
    , {max_retries, mk(non_neg_integer(), #{required => false, desc => ?DESC("max_retries")})}
    , {max_block_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"250mb">>,
                    importance => ?IMPORTANCE_HIDDEN,
                    required => true
                }
            )}
    , {min_block_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"100mb">>,
                    importance => ?IMPORTANCE_HIDDEN,
                    required => true
                }
            )}
    ];
fields(direct_parameters) ->
    [ {mode, mk(direct, #{required => true, desc => ?DESC("direct_mode")})}
    %% todo
    ];
fields(aggregation) ->
    [
        emqx_connector_aggregator_schema:container(),
        %% TODO: Needs bucketing? (e.g. messages falling in this 1h interval)
        {time_interval,
            hoconsc:mk(
                emqx_schema:duration_s(),
                #{
                    required => false,
                    default => <<"1h">>,
                    desc => ?DESC("aggregation_interval")
                }
            )},
        {max_records,
            hoconsc:mk(
                pos_integer(),
                #{
                    required => false,
                    default => 1_000_000,
                    desc => ?DESC("aggregation_max_records")
                }
            )}
    ];
fields(action_resource_opts) ->
    Fields = emqx_bridge_v2_schema:action_resource_opts_fields(),
    lists:foldl(fun proplists:delete/2, Fields, [batch_size, batch_time]).

desc(Name) when
    Name =:= ?ACTION_TYPE;
    Name =:= parameters
->
    ?DESC(Name);
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            ?ACTION_TYPE_BIN => #{
                summary => <<"Snowflake Action">>,
                value => action_example(Method)
            }
        }
    ].

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => ?ACTION_TYPE_BIN,
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
        description => <<"my action">>,
        connector => <<"my_connector">>,
        parameters =>
            #{
                sql => <<"insert into mqtt (key, value) values (${.id}, ${.payload})">>
            },
        resource_opts =>
            #{
                %% batch is not yet supported
                %% batch_time => <<"0ms">>,
                %% batch_size => 1,
                health_check_interval => <<"30s">>,
                inflight_window => 100,
                query_mode => <<"sync">>,
                request_ttl => <<"45s">>,
                worker_pool_size => 16
            }
    }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

ref(Name) -> hoconsc:ref(?MODULE, Name).
mk(Type, Meta) -> hoconsc:mk(Type, Meta).

mkunion(Field, Schemas) ->
    hoconsc:union(fun(Arg) -> scunion(Field, Schemas, Arg) end).

scunion(_Field, Schemas, all_union_members) ->
    maps:values(Schemas);
scunion(Field, Schemas, {value, Value}) ->
    Selector = maps:get(emqx_utils_conv:bin(Field), Value, undefined),
    case Selector == undefined orelse maps:find(emqx_utils_conv:bin(Selector), Schemas) of
        {ok, Schema} ->
            [Schema];
        _Error ->
            throw(#{field_name => Field, expected => maps:keys(Schemas)})
    end.
