%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_impl_producer).

-include("emqx_bridge_pulsar.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    is_buffer_supported/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3
]).

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------

%% FIXME: will need to patch pulsar to support per-request callback
%% fns....
callback_mode() -> always_sync.

%% there are no queries to be made to this bridge, so we say that
%% buffer is supported so we don't spawn unused resource buffer
%% workers.
is_buffer_supported() -> true.

on_start(InstanceId, Config) ->
    #{
        authentication := _Auth,
        bridge_name := BridgeName,
        servers := Servers0,
        ssl := SSL
    } = Config,
    Servers = format_servers(Servers0),
    ClientId = make_client_id(InstanceId, BridgeName),
    SSLOpts = emqx_tls_lib:to_client_opts(SSL),
    ClientOpts = #{
        ssl_opts => SSLOpts,
        conn_opts => conn_opts(Config)
    },
    case pulsar:ensure_supervised_client(ClientId, Servers, ClientOpts) of
        {ok, _Pid} ->
            ?SLOG(info, #{
                msg => "pulsar_client_started",
                instance_id => InstanceId,
                pulsar_hosts => Servers
            });
        {error, Error} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_client",
                instance_id => InstanceId,
                pulsar_hosts => Servers,
                reason => Error
            }),
            throw(failed_to_start_pulsar_client)
    end,
    start_producer(Config, InstanceId, ClientId, ClientOpts).

on_stop(_InstanceId, State) ->
    #{
        pulsar_client_id := ClientId,
        producers := Producers
    } = State,
    stop_producers(ClientId, Producers),
    stop_client(ClientId),
    ?tp(pulsar_bridge_stopped, #{instance_id => _InstanceId}),
    ok.

on_get_status(_InstanceId, State) ->
    #{pulsar_client_id := ClientId} = State,
    case pulsar_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            try pulsar_client:get_status(Pid) of
                true ->
                    connected;
                false ->
                    disconnected
            catch
                error:timeout ->
                    disconnected
            end;
        {error, _} ->
            disconnected
    end.

on_query(_InstanceId, {send_message, Message}, State) ->
    #{
        producers := Producers,
        sync_timeout := SyncTimeout,
        message_template := MessageTemplate
    } = State,
    PulsarMessage = render_message(Message, MessageTemplate),
    pulsar:send_sync(Producers, [PulsarMessage], SyncTimeout).

%% FIXME: will need to patch pulsar to support per-request callback
%% fns....
%% on_query_async(_InstanceId, {send_message, _Message}, _AsyncReplyFn, State) ->
%%     #{producers := Producers} = State,
%%     Message = #{key => <<"fixme">>, value => <<"fixme">>},
%%     %% FIXME: patch pulsar to return worker pid
%%     pulsar:send(Producers, [Message]).

%%-------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(L) when is_list(L) ->
    list_to_binary(L);
to_bin(B) when is_binary(B) ->
    B.

format_servers(Servers0) ->
    Servers1 = emqx_schema:parse_servers(Servers0, ?PULSAR_HOST_OPTIONS),
    lists:map(
        fun({Scheme, Host, Port}) ->
            Scheme ++ "://" ++ Host ++ ":" ++ integer_to_list(Port)
        end,
        Servers1
    ).

make_client_id(InstanceId, BridgeName) ->
    case is_dry_run(InstanceId) of
        true ->
            pulsar_producer_probe;
        false ->
            ClientIdBin = iolist_to_binary([
                <<"pulsar_producer:">>,
                to_bin(BridgeName),
                <<":">>,
                to_bin(node())
            ]),
            binary_to_atom(ClientIdBin)
    end.

is_dry_run(InstanceId) ->
    TestIdStart = string:find(InstanceId, ?TEST_ID_PREFIX),
    case TestIdStart of
        nomatch ->
            false;
        _ ->
            string:equal(TestIdStart, InstanceId)
    end.

conn_opts(#{authentication := none}) ->
    #{};
conn_opts(#{authentication := #{username := Username, password := Password}}) ->
    #{
        auth_data => iolist_to_binary([Username, <<":">>, Password]),
        auth_method_name => <<"basic">>
    };
conn_opts(#{authentication := #{jwt := JWT}}) ->
    #{
        auth_data => JWT,
        auth_method_name => <<"token">>
    }.

replayq_dir(ClientId) ->
    filename:join([emqx:data_dir(), "pulsar", ClientId]).

producer_name(ClientId) ->
    ClientIdBin = to_bin(ClientId),
    binary_to_atom(
        iolist_to_binary([
            <<"producer-">>,
            ClientIdBin
        ])
    ).

start_producer(Config, InstanceId, ClientId, ClientOpts) ->
    #{
        conn_opts := ConnOpts,
        ssl_opts := SSLOpts
    } = ClientOpts,
    #{
        batch_size := BatchSize,
        buffer := #{
            mode := BufferMode,
            per_partition_limit := PerPartitionLimit,
            segment_bytes := SegmentBytes,
            memory_overload_protection := MemOLP0
        },
        compression := Compression,
        max_batch_bytes := MaxBatchBytes,
        message := MessageTemplateOpts,
        pulsar_topic := PulsarTopic0,
        retention_period := RetentionPeriod,
        send_buffer := SendBuffer,
        strategy := Strategy,
        sync_timeout := SyncTimeout
    } = Config,
    {OffloadMode, ReplayQDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(ClientId)};
            hybrid -> {true, replayq_dir(ClientId)}
        end,
    MemOLP =
        case os:type() of
            {unix, linux} -> MemOLP0;
            _ -> false
        end,
    ReplayQOpts = #{
        replayq_dir => ReplayQDir,
        replayq_offload_mode => OffloadMode,
        replayq_max_total_bytes => PerPartitionLimit,
        replayq_seg_bytes => SegmentBytes,
        drop_if_highmem => MemOLP
    },
    ProducerName = producer_name(ClientId),
    MessageTemplate = compile_message_template(MessageTemplateOpts),
    ProducerOpts0 =
        #{
            batch_size => BatchSize,
            %% FIXME: need to patch pulsar driver to allow per-request
            %% callbacks
            %% callback => {?MODULE, pulsar_callback, []},
            compression => Compression,
            conn_opts => ConnOpts,
            max_batch_bytes => MaxBatchBytes,
            name => ProducerName,
            retention_period => RetentionPeriod,
            ssl_opts => SSLOpts,
            strategy => Strategy,
            tcp_opts => [{sndbuf, SendBuffer}]
        },
    ProducerOpts = maps:merge(ReplayQOpts, ProducerOpts0),
    PulsarTopic = binary_to_list(PulsarTopic0),
    case pulsar:ensure_supervised_producers(ClientId, PulsarTopic, ProducerOpts) of
        {ok, Producers} ->
            State = #{
                pulsar_client_id => ClientId,
                producers => Producers,
                sync_timeout => SyncTimeout,
                message_template => MessageTemplate
            },
            {ok, State};
        {error, Error} ->
            ?SLOG(error, #{
                msg => "failed_to_start_pulsar_producer",
                instance_id => InstanceId,
                reason => Error
            }),
            stop_client(ClientId),
            throw(failed_to_start_pulsar_producer)
    end.

stop_client(ClientId) ->
    _ = log_when_error(
        fun() ->
            ok = pulsar:stop_and_delete_supervised_client(ClientId),
            ?tp(pulsar_bridge_client_stopped, #{pulsar_client_id => ClientId}),
            ok
        end,
        #{
            msg => "failed_to_delete_pulsar_client",
            pulsar_client_id => ClientId
        }
    ),
    ok.

stop_producers(ClientId, Producers) ->
    _ = log_when_error(
        fun() ->
            ok = pulsar:stop_and_delete_supervised_producers(Producers),
            ?tp(pulsar_bridge_producer_stopped, #{pulsar_client_id => ClientId}),
            ok
        end,
        #{
            msg => "failed_to_delete_pulsar_producer",
            pulsar_client_id => ClientId
        }
    ),
    ok.

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

compile_message_template(TemplateOpts) ->
    KeyTemplate = maps:get(key, TemplateOpts, <<"${.clientid}">>),
    ValueTemplate = maps:get(value, TemplateOpts, <<"${.}">>),
    #{
        key => preproc_tmpl(KeyTemplate),
        value => preproc_tmpl(ValueTemplate)
    }.

preproc_tmpl(Template) ->
    emqx_plugin_libs_rule:preproc_tmpl(Template).

render_message(
    Message, #{key := KeyTemplate, value := ValueTemplate}
) ->
    #{
        key => render(Message, KeyTemplate),
        value => render(Message, ValueTemplate)
    }.

render(Message, Template) ->
    Opts = #{
        var_trans => fun
            (undefined) -> <<"">>;
            (X) -> emqx_plugin_libs_rule:bin(X)
        end,
        return => full_binary
    },
    emqx_plugin_libs_rule:proc_tmpl(Template, Message, Opts).
