%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_connector).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).
-behaviour(emqx_connector_aggreg_delivery).

-include_lib("public_key/include/public_key.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_snowflake.hrl").
-include_lib("emqx_connector_aggregator/include/emqx_connector_aggregator.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3
    %% on_batch_query/3
]).

%% `ecpool_worker' API
-export([connect/1, disconnect/1]).

%% `emqx_connector_aggreg_delivery' API
-export([
    init_transfer_state/2,
    process_append/2,
    process_write/1,
    process_complete/1
]).

%% API
-export([
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SUCCESS(STATUS_CODE), (STATUS_CODE >= 200 andalso STATUS_CODE < 300)).

%% Ad-hoc requests
-record(sql_query, {sql :: sql(), opts :: ad_hoc_query_opts()}).

-type connector_config() :: #{
}.
-type connector_state() :: #{
    installed_actions := #{action_resource_id() => action_state()}
}.

-type action_config() :: #{
}.
-type action_state() :: #{
}.

-type query() :: action_query() | ad_hoc_query().
-type action_query() :: {_Tag :: channel_id(), _Data :: map()}.
-type ad_hoc_query() :: #sql_query{}.

-type sql() :: iolist().
-type ad_hoc_query_opts() :: map().

-type transfer_opts() :: #{
    %% upload_options := #{
    %%     action := binary(),
    %%     blob := emqx_template:t(),
    %%     container := string(),
    %%     min_block_size := pos_integer(),
    %%     max_block_size := pos_integer(),
    %%     driver_state := driver_state()
    %% }
}.

-type transfer_buffer() :: iolist().

-type transfer_state() :: #{
    %% blob := blob(),
    %% buffer := transfer_buffer(),
    %% buffer_size := non_neg_integer(),
    %% container := container(),
    %% max_block_size := pos_integer(),
    %% min_block_size := pos_integer(),
    %% next_block := queue:queue(iolist()),
    %% num_blocks := non_neg_integer(),
    %% driver_state := driver_state(),
    %% started := boolean()
}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    #{ server := Server
     , account := Account
     , username := Username
     , password := Password
     , dsn := DSN
     , pool_size := PoolSize
     } = ConnConfig,
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?SERVER_OPTS),
    PoolOpts = [ {pool_size, PoolSize}
               , {dsn, DSN}
               , {account, Account}
               , {server, Server}
               , {username, Username}
               , {password, Password}
               , {on_disconnect, {?MODULE, disconnect, []}}
               ],
    case emqx_resource_pool:start(ConnResId, ?MODULE, PoolOpts) of
        ok ->
            State = #{
                      account => Account,
                      server => #{host => Host, port => Port},
                      installed_actions => #{}
                     },
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    Res = emqx_resource_pool:stop(ConnResId),
    ?tp("snowflake_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, _ConnState) ->
    todo,
    ?status_connected.

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ActionResId, ActionConfig) ->
    maybe
        {ok, ActionState} ?= create_action(ActionResId, ActionConfig, ConnState0),
        ConnState = emqx_utils_maps:deep_put([installed_actions, ActionResId], ConnState0, ActionState),
        {ok, ConnState}
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId, ConnState0 = #{installed_actions := InstalledActions0}, ActionResId
) when
    is_map_key(ActionResId, InstalledActions0)
->
    {ActionState, InstalledActions} = maps:take(ActionResId, InstalledActions0),
    destroy_action(ActionResId, ActionState),
    ConnState = ConnState0#{installed_actions := InstalledActions},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ActionResId) ->
    {ok, ConnState}.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), action_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ActionResId,
    _ConnState = #{installed_actions := InstalledActions}
) when is_map_key(ActionResId, InstalledActions) ->
    todo,
    ?status_connected;
on_get_channel_status(_ConnResId, _ActionResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(ConnResId, {Tag, Data}, #{installed_actions := InstalledActions} = ConnState) when
    is_map_key(Tag, InstalledActions)
->
    {ok, todo};
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

%% -spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
%%     {ok, _Result} | {error, _Reason}.
%% on_batch_query(_ConnResId, [{Tag, _} | Rest], #{installed_actions := InstalledActions}) when
%%     is_map_key(Tag, InstalledActions)
%% ->
%%     ActionState = maps:get(Tag, InstalledActions),
%%     todo;
%% on_batch_query(_ConnResId, Batch, _ConnState) ->
%%     {error, {unrecoverable_error, {bad_batch, Batch}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `ecpool_worker' API
%%------------------------------------------------------------------------------

connect(Opts) ->
    ConnectStr = conn_str(Opts),
    DriverOpts = proplists:get_value(driver_options, Opts, []),
    odbc:connect(ConnectStr, DriverOpts).

disconnect(ConnectionPid) ->
    odbc:disconnect(ConnectionPid).

do_stage_file(ODBCPool, Filename, Database, Schema, Stage) ->
    SQL0 = iolist_to_binary([ <<"PUT file://">>
                            , Filename
                            , <<" @">>
                                  %% TODO: escape names?
                            , Database, <<".">>, Schema, <<".">>, Stage
                            ]),
    SQL = binary_to_list(SQL0),
    Res = ecpool:pick_and_do(
      ODBCPool,
      fun(ConnPid) ->
         %% TODO: check if it actually succeeded?
         odbc:sql_query(ConnPid, SQL)
      end,
            no_handover),
    case Res of
        {updated, _} ->
            ok;
        {error, Reason} ->
            ?SLOG(warning, #{ msg => "snowflake_stage_file_failed"
                            , reason => Reason
                            , filename => Filename
                            , database => Database
                            , schema => Schema
                            , stage => Stage
                            , pool => ODBCPool
                            }),
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

-spec init_transfer_state(buffer(), transfer_opts()) ->
    transfer_state().
init_transfer_state(Buffer, Opts) ->
    #{
        action_res_id := ActionResId,
        container := #{type := ContainerType},
        http_client_config := HTTPClientConfig,
        upload_options := #{
            max_block_size := MaxBlockSize,
            min_block_size := MinBlockSize
        },
        work_dir := WorkDir
    } = Opts,
    FilenameTemplate = emqx_template:parse(<<"${action_res_id}_${seq_no}.${container_type}">>),
    #{
        action_res_id => ActionResId,
        buffer_ctx => Buffer,

        seq_no => 0,
        container_type => ContainerType,

        http_pool => ActionResId,
        http_client_config => HTTPClientConfig,

        odbc_pool => ActionResId,
        filename_template => FilenameTemplate,
        filename => undefined,
        fd => undefined,
        work_dir => WorkDir,
        written => 0,
        staged_files => [],
        next_file => queue:new(),

        max_block_size => MaxBlockSize,
        min_block_size => MinBlockSize
    }.

-spec process_append(iodata(), transfer_state()) ->
    transfer_state().
process_append(IOData, TransferState0) ->
    #{min_block_size := MinBlockSize} = TransferState0,
    Size = iolist_size(IOData),
    %% Open and write to file until minimum is reached
    TransferState1 = ensure_file(TransferState0),
    #{written := Written} = TransferState2 = append_to_file(IOData, Size, TransferState1),
    case Written >= MinBlockSize of
        true ->
            close_and_enqueue_file(TransferState2);
        false ->
            TransferState2
    end.

ensure_file(#{fd := undefined} = TransferState) ->
    #{
        action_res_id := ActionResId,
        container_type := ContainerType,
        filename_template := FilenameTemplate,
        seq_no := SeqNo,
        work_dir := WorkDir
     } = TransferState,
    Filename0 = emqx_template:render_strict(FilenameTemplate, #{
                                                               action_res_id => ActionResId,
                                                               seq_no => SeqNo,
                                                               container_type => ContainerType
                                                              }),
    Filename = filename:join([WorkDir, <<"tmp">>, Filename0]),
    {ok, FD} = file:open(Filename, [write, binary]),
    TransferState#{
                   filename := Filename,
                   fd := FD
                  };
ensure_file(TransferState) ->
    TransferState.

append_to_file(IOData, Size, TransferState) ->
    #{ fd := FD
     , written := Written
     } = TransferState,
    %% TODO: handle errors
    ok = file:write(FD, IOData),
    TransferState#{written := Written + Size}.

close_and_enqueue_file(TransferState0) ->
    #{ fd := FD
     , filename := Filename
     , next_file := NextFile
     , seq_no := SeqNo
     , written := Written
     } = TransferState0,
    ok = file:close(FD),
    TransferState0#{
                    next_file := queue:in({Filename, Written}, NextFile),
                    filename := undefined,
                    fd := undefined,
                    seq_no := SeqNo + 1,
                    written := 0
                   }.

-spec process_write(transfer_state()) ->
    {ok, transfer_state()} | {error, term()}.
process_write(TransferState0) ->
    #{next_file := NextFile0} = TransferState0,
    case queue:out(NextFile0) of
        {{value, {Filename, Size}}, NextFile} ->
            ?tp(snowflake_will_stage_file, #{}),
            do_process_write(Filename, Size, TransferState0#{next_file := NextFile});
        {empty, _} ->
            {ok, TransferState0}
    end.

do_process_write(Filename, Size, TransferState0) ->
    #{
      odbc_pool := ODBCPool,
      database := Database,
      schema := Schema,
      stage := Stage,
      staged_files := StagedFiles0
     } = TransferState0,
    case do_stage_file(ODBCPool, Filename, Database, Schema, Stage) of
        {ok, _} ->
            Basename = filename:basename(Filename),
            SFPath = filename:join(Stage, Basename),
            StagedFile = #{path => SFPath, size => Size},
            StagedFiles = [StagedFile | StagedFiles0],
            TransferState = TransferState0#{staged_files := StagedFiles},
            process_write(TransferState);
        {error, Reason} ->
            {error, Reason}
    end.

-spec process_complete(transfer_state()) ->
    {ok, term()}.
process_complete(TransferState0) ->
    maybe
        %% Flush any left-over data
        {ok, TransferState} ?= process_write(TransferState0),
        #{ http_pool := HTTPPool
         , http_client_config := HTTPClientConfig
         , staged_files := StagedFiles
         } = TransferState,
        case do_insert_files_request(StagedFiles, HTTPPool, HTTPClientConfig) of
            _ ->
                ok
        end
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec create_action(action_resource_id(), action_config(), connector_state()) ->
          {ok, action_state()} | {error, term()}.
create_action(ActionResId, #{parameters := #{mode := aggregated}} = ActionConfig, ConnState) ->
    ct:pal("~p>>>>>>>>>\n  ~p",[{node(),?MODULE,?LINE},#{cfg => ActionConfig}]),
    maybe
        {ok, ActionState0} ?= start_http_pool(ActionResId, ActionConfig, ConnState),
        start_aggregator(ActionResId, ActionConfig, ActionState0)
    end.

start_http_pool(ActionResId, ActionConfig, ConnState) ->
    #{ server := #{host := Host, port := Port}
     } = ConnState,
    #{ parameters := #{ database := Database
                      , schema := Schema
                      , pipe := Pipe
                      , pipe_user := _
                      , private_key := _
                      , connect_timeout := ConnectTimeout
                      , pipelining := Pipelining
                      , pool_size := PoolSize
                      , max_retries := MaxRetries
                      }
     , resource_opts := #{ request_ttl := RequestTTL
                         }
     } = ActionConfig,
    PipePath0 = iolist_to_binary(lists:join($., [Database, Schema, Pipe])),
    PipePath = string:uppercase(PipePath0),
    InserFilesPath = iolist_to_binary([ <<"https://">>
                                      , Host
                                      , <<":">>
                                      , integer_to_binary(Port)
                                      , <<"/v1/data/pipes/">>
                                      , PipePath
                                      , <<"/insertFiles">>
                                      ]),
    JWTConfig = jwt_config(ActionResId, ActionConfig, ConnState),
    TransportOpts = emqx_tls_lib:to_client_opts(#{enable => true, verify => verify_none}),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30_000},
        {pool_type, random},
        {pool_size, PoolSize},
        {transport, tls},
        {transport_opts, TransportOpts},
        {enable_pipelining, Pipelining}
    ],
    case ehttpc_sup:start_pool(ActionResId, PoolOpts) of
        {ok, _} ->
            {ok, #{http => #{ jwt_config => JWTConfig
                            , insert_files_path => InserFilesPath
                            , max_retries => MaxRetries
                            , request_ttl => RequestTTL
                            }}};
        {error, Reason} ->
            {error, Reason}
    end.

start_aggregator(ActionResId, ActionConfig, ActionState0) ->
    ct:pal("~p>>>>>>>>>\n  ~p",[{node(),?MODULE,?LINE},#{cfg => ActionConfig}]),
    maybe
    #{ bridge_name := Name
     , parameters := #{
                       mode := aggregated = Mode,
                       aggregation := #{
                                        container := ContainerOpts,
                                        max_records := MaxRecords,
                                        time_interval := TimeInterval
                                       },
                       max_block_size := MaxBlockSize,
                       min_block_size := MinBlockSize
                      }
     } = ActionConfig,
    #{http := HTTPClientConfig} = ActionState0,
    Type = ?ACTION_TYPE_BIN,
    AggregId = {Type, Name},
    WorkDir = work_dir(Type, Name),
    AggregOpts = #{
        max_records => MaxRecords,
        time_interval => TimeInterval,
        work_dir => WorkDir
    },
    TransferOpts = #{
        action => Name,
        action_res_id => ActionResId,
        http_client_config => HTTPClientConfig,
        max_block_size => MaxBlockSize,
        min_block_size => MinBlockSize,
        work_dir => WorkDir
    },
    DeliveryOpts = #{
        callback_module => ?MODULE,
        container => ContainerOpts,
        upload_options => TransferOpts
    },
    _ = ?AGGREG_SUP:delete_child(AggregId),
    {ok, SupPid} ?= ?AGGREG_SUP:start_child(#{
        id => AggregId,
        start =>
            {emqx_connector_aggreg_upload_sup, start_link, [AggregId, AggregOpts, DeliveryOpts]},
        type => supervisor,
        restart => permanent
    }),
    {ok, ActionState0#{ mode => Mode
                      , aggreg_id => AggregId
                      , supervisor => SupPid
                      , on_stop => {?AGGREG_SUP, delete_child, [AggregId]}
                      }}
    else
        {error, Reason} ->
            ehttpc_sup:stop_pool(ActionResId),
            {error, Reason}
    end.

-spec destroy_action(action_resource_id(), action_state()) -> ok.
destroy_action(ActionResId, _ActionState) ->
    ok = ehttpc_sup:stop_pool(ActionResId),
    ok.

work_dir(Type, Name) ->
    filename:join([emqx:data_dir(), bridge, Type, Name]).

str(X) -> emqx_utils_conv:str(X).

conn_str(Opts) ->
    lists:concat(conn_str(Opts, [])).

conn_str([], Acc) ->
    lists:join(";", Acc);
conn_str([{dsn, DSN} | Opts], Acc) ->
    conn_str(Opts, ["dsn=" ++ str(DSN) | Acc]);
conn_str([{server, Server} | Opts], Acc) ->
    conn_str(Opts, ["server=" ++ str(Server) | Acc]);
conn_str([{account, Account} | Opts], Acc) ->
    conn_str(Opts, ["account=" ++ str(Account) | Acc]);
conn_str([{username, Username} | Opts], Acc) ->
    conn_str(Opts, ["uid=" ++ str(Username) | Acc]);
conn_str([{password, Password} | Opts], Acc) ->
    conn_str(Opts, ["pwd=" ++ str(emqx_secret:unwrap(Password)) | Acc]);
conn_str([{_, _} | Opts], Acc) ->
    conn_str(Opts, Acc).

jwt_config(ActionResId, ActionConfig, ConnState) ->
    #{ account := Account
     } = ConnState,
    #{ parameters := #{ private_key := PrivateKeyPEM
     , pipe_user := PipeUser
     }
     } = ActionConfig,
    PrivateJWK = jose_jwk:from_pem(emqx_secret:unwrap(PrivateKeyPEM)),
    AccountUp = string:uppercase(Account),
    PipeUserUp = string:uppercase(PipeUser),
    Fingerprint = fingerprint(PrivateJWK),
    Sub = iolist_to_binary([AccountUp, <<".">>, PipeUserUp]),
    Iss = iolist_to_binary([Sub, <<".">>, Fingerprint]),
    #{ expiration => 360_000
     , resource_id => ActionResId
     , jwk => emqx_secret:wrap(PrivateJWK)
     , iss => Iss
     , sub => Sub
     , aud => <<"unused">>
     , kid => <<"unused">>
     , alg => <<"RS256">>
     }.

fingerprint(PrivateJWK) ->
    {_, PublicRSAKey} = jose_jwk:to_public_key(PrivateJWK),
    #'SubjectPublicKeyInfo'{algorithm = DEREncoded} =
        public_key:pem_entry_encode('SubjectPublicKeyInfo', PublicRSAKey),
    Hash = crypto:hash(sha256, DEREncoded),
    Hash64 = base64:encode(Hash),
    <<"SHA256:", Hash64/binary>>.

do_insert_files_request(Filenames, HTTPPool, HTTPClientConfig) ->
    #{ jwt_config := JWTConfig
     , insert_files_path := InserFilesPath
     , request_ttl := RequestTTL
     , max_retries := MaxRetries
     } = HTTPClientConfig,
    JWTToken = emqx_connector_jwt:ensure_jwt(JWTConfig),
    AuthnHeader = [<<"BEARER ">>, JWTToken],
    Headers = [ {<<"X-Snowflake-Authorization-Token-Type">>, <<"KEYPAIR_JWT">>}
              , {<<"Content-Type">>, <<"application/json">>}
              , {<<"Authorization">>, AuthnHeader}
              ],
    Files = lists:map(
             fun(Filename) ->
                     %% FIXME: different compressions?
                     #{path => iolist_to_binary([Filename, <<".gz">>])}
             end,
             Filenames),
    Body = emqx_utils_json:encode(#{files => Files}),
    Req = {InserFilesPath, Headers, Body},
    Response = ehttpc:request(
                 HTTPPool,
                 post,
                 Req,
                 RequestTTL,
                 MaxRetries
                ),
    Response.
