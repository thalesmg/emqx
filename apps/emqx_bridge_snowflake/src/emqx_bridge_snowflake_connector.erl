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
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3
]).

%% `ecpool_worker' API
-export([connect/1, disconnect/1, do_health_check_connector/1]).

%% `emqx_connector_aggreg_delivery' API
-export([
    init_transfer_state/2,
    process_append/2,
    process_write/1,
    process_complete/1
]).

%% API
-export([
   insert_report/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SUCCESS(STATUS_CODE), (STATUS_CODE >= 200 andalso STATUS_CODE < 300)).

%% Ad-hoc requests
-record(insert_report, {action_res_id :: action_resource_id(), opts :: ad_hoc_query_opts()}).

-type connector_config() :: #{
  server := binary(),
  account := account(),
  username := binary(),
  password := emqx_schema_secret:secret(),
  dsn := binary(),
  pool_size := pos_integer()
}.
-type connector_state() :: #{
    account := account(),
    server := #{host := binary(), port := emqx_schema:port_number()},
    installed_actions := #{action_resource_id() => action_state()}
}.

-type action_config() :: aggregated_action_config().
-type aggregated_action_config() :: #{
  parameters := #{
    mode := aggregated
  , database := database()
  , schema := schema()
  , pipe := pipe()
  , pipe_user := binary()
  , private_key := emqx_schema_secret:secret()
  , connect_timeout := emqx_schema:timeout_duration()
  , pipelining := non_neg_integer()
  , pool_size := pos_integer()
  , max_retries := non_neg_integer()
  }
}.
-type action_state() :: #{
}.

-type account() :: binary().
-type database() :: binary().
-type schema() :: binary().
-type stage() :: binary().
-type pipe() :: binary().

-type odbc_pool() :: connector_resource_id().
-type http_pool() :: action_resource_id().
-type http_client_config() :: #{
                                jwt_config := emqx_connector_jwt:jwt_config()
                               , insert_files_path := binary()
                               , insert_report_path := binary()
                               , max_retries := non_neg_integer()
                               , request_ttl := timeout()
}.

-type query() :: action_query() | insert_report_query().
-type action_query() :: {_Tag :: channel_id(), _Data :: map()}.
-type insert_report_query() :: #insert_report{}.

-type ad_hoc_query_opts() :: map().

-type transfer_opts() :: #{
    container := #{type := emqx_connector_aggregator:container_type()},
    upload_options := #{
        action_res_id := action_resource_id(),
      database := database(),
      schema := schema(),
      stage := stage(),
        odbc_pool := odbc_pool(),
        http_pool := http_pool(),
        http_client_config := http_client_config(),
        min_block_size := pos_integer(),
        max_block_size := pos_integer(),
        work_dir := file:filename()
    }
}.

-type transfer_buffer() :: iolist().

-type transfer_state() :: #{
        action_res_id := action_resource_id(),

        seq_no := non_neg_integer(),
        container_type := emqx_connector_aggregator:container_type(),

        http_pool := http_pool(),
        http_client_config := http_client_config(),

        odbc_pool := odbc_pool(),
      database := database(),
      schema := schema(),
      stage := stage(),
        filename_template := emqx_template:t(),
        filename := emqx_maybe:t(file:filename()),
        fd := emqx_maybe:t(file:io_device()),
        work_dir := file:filename(),
        written := non_neg_integer(),
        staged_files := [staged_file()],
        next_file := queue:queue({file:filename(), non_neg_integer()}),

        max_block_size := pos_integer(),
        min_block_size := pos_integer()
}.
-type staged_file() :: #{
  path := file:filename(),
  size := non_neg_integer()
}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    snowflake.

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
    health_check_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(ConnResId, ConnState0, ActionResId, ActionConfig) ->
    maybe
        {ok, ActionState} ?= create_action(ConnResId, ActionResId, ActionConfig, ConnState0),
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
    ActionState = maps:get(ActionResId, InstalledActions),
    action_status(ActionState);
on_get_channel_status(_ConnResId, _ActionResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(_ConnResId, {ActionResId, Data}, #{installed_actions := InstalledActions} = ConnState) when
    is_map_key(ActionResId, InstalledActions)
->
    case InstalledActions of
        #{ActionResId := #{mode := aggregated} = ActionState} ->
            run_aggregated_action([Data], ActionState);
        #{ActionResId := #{mode := direct} = _ActionState} ->
            {ok, todo}
        end;
on_query(_ConnResId, #insert_report{action_res_id = ActionResId, opts = Opts}, #{installed_actions := InstalledActions} = _ConnState) when
    is_map_key(ActionResId, InstalledActions)
->
    #{mode := aggregated, http := HTTPClientConfig} = maps:get(ActionResId, InstalledActions),
    do_insert_report_request(ActionResId, Opts, HTTPClientConfig);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_batch_query(_ConnResId, [{ActionResId, _} | _] = Batch0, #{installed_actions := InstalledActions}) when
    is_map_key(ActionResId, InstalledActions)
->
    case InstalledActions of
        #{ActionResId := #{mode := aggregated} = ActionState} ->
            Batch = [Data || {_, Data} <- Batch0],
            run_aggregated_action(Batch, ActionState);
        #{ActionResId := #{mode := direct} = _ActionState} ->
            {ok, todo}
    end;
on_batch_query(_ConnResId, Batch, _ConnState) ->
    {error, {unrecoverable_error, {bad_batch, Batch}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec insert_report(action_resource_id(), _Opts :: map()) -> _TODO.
insert_report(ActionResId, Opts) ->
    emqx_resource:simple_sync_query(ActionResId, #insert_report{action_res_id = ActionResId, opts = Opts}).

%%------------------------------------------------------------------------------
%% `ecpool_worker' API
%%------------------------------------------------------------------------------

connect(Opts) ->
    ConnectStr = conn_str(Opts),
    DriverOpts = proplists:get_value(driver_options, Opts, []),
    odbc:connect(ConnectStr, DriverOpts).

disconnect(ConnectionPid) ->
    odbc:disconnect(ConnectionPid).

health_check_connector(ConnResId) ->
    Res = emqx_resource_pool:health_check_workers(
            ConnResId,
            fun ?MODULE:do_health_check_connector/1),
    case Res of
        true ->
            ?status_connected;
        false ->
            ?status_disconnected
    end.

do_health_check_connector(ConnectionPid) ->
    case odbc:sql_query(ConnectionPid, "show schemas") of
        {selected, _, _} ->
            true;
        _ ->
            false
    end.

-spec do_stage_file(odbc_pool(), file:filename(), database(), schema(), stage()) ->
          ok | {error, term()}.
do_stage_file(ODBCPool, Filename, Database, Schema, Stage) ->
    SQL0 = iolist_to_binary([ <<"PUT file://">>
                                  %% TODO: use action as directory name on stage?
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
              'Elixir.IO':inspect(#{me => self(), odbc_pid_inside => ConnPid}, []),
         odbc:sql_query(ConnPid, SQL)
      end,
            %% Must be executed by the ecpool worker, which owns the ODBC connection.
            handover),
    Context = #{
                   filename => Filename
                  , database => Database
                  , schema => Schema
                  , stage => Stage
                  , pool => ODBCPool
                  },
    handle_stage_file_result(Res, Context).

handle_stage_file_result({selected, Headers0, Rows}, Context) ->
    #{filename := Filename} = Context,
    Headers = lists:map(fun emqx_utils_conv:bin/1, Headers0),
    ParsedRows = lists:map(fun(R) -> row_to_map(R, Headers) end, Rows),
    case ParsedRows of
        [#{<<"target">> := Target, <<"status">> := <<"UPLOADED">>}] ->
            ?SLOG(debug, Context#{ msg => "snowflake_stage_file_succeeded"
                                 , result => ParsedRows
                                 }),
            ok = file:delete(Filename),
            {ok, Target};
        [#{<<"target">> := Target, <<"status">> := <<"SKIPPED">>}] ->
            ?SLOG(info, Context#{ msg => "snowflake_stage_file_skipped"
                                   , result => ParsedRows
                                   }),
            ok = file:delete(Filename),
            {ok, Target};
        _ ->
            {error, {bad_response, ParsedRows}}
    end;
handle_stage_file_result({error, Reason} = Error, Context) ->
    ?SLOG(warning, Context#{ msg => "snowflake_stage_file_failed"
                              , reason => Reason
                              }),
    Error.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

-spec init_transfer_state(buffer(), transfer_opts()) ->
    transfer_state().
init_transfer_state(_Buffer, Opts) ->
    #{
        container := #{type := ContainerType},
        upload_options := #{
            action_res_id := ActionResId,
            database := Database,
            schema := Schema,
            stage := Stage,
            odbc_pool := ODBCPool,
            http_pool := HTTPPool,
            http_client_config := HTTPClientConfig,
            max_block_size := MaxBlockSize,
            min_block_size := MinBlockSize,
            work_dir := WorkDir
        }
    } = Opts,
    FilenameTemplate = emqx_template:parse(<<"${action_res_id}_${seq_no}.${container_type}">>),
    #{
        action_res_id => ActionResId,

        seq_no => 0,
        container_type => ContainerType,

        http_pool => HTTPPool,
        http_client_config => HTTPClientConfig,

        odbc_pool => ODBCPool,
      database => Database,
      schema => Schema,
      stage => Stage,
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
            'Elixir.IO':inspect(#{}, []),
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
    ok = filelib:ensure_dir(Filename),
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

-spec do_process_write(file:filename(), non_neg_integer(), transfer_state()) ->
    {ok, transfer_state()} | {error, term()}.
do_process_write(Filename, Size, TransferState0) ->
    #{
      odbc_pool := ODBCPool,
      database := Database,
      schema := Schema,
      stage := Stage,
      staged_files := StagedFiles0
     } = TransferState0,
    case do_stage_file(ODBCPool, Filename, Database, Schema, Stage) of
        {ok, Target} ->
            StagedFile = #{path => Target, size => Size},
            StagedFiles = [StagedFile | StagedFiles0],
            TransferState = TransferState0#{staged_files := StagedFiles},
            process_write(TransferState);
        {error, Reason} ->
            {error, Reason}
    end.

-spec process_complete(transfer_state()) ->
    {ok, term()}.
process_complete(TransferState0) ->
    #{written := Written0} = TransferState0,
    maybe
        %% Flush any left-over data
        {ok, TransferState} ?=
            case Written0 > 0 of
                true ->
                    TransferState1 = close_and_enqueue_file(TransferState0),
                    process_write(TransferState1);
                false ->
                    {ok, TransferState0}
            end,
        #{ http_pool := HTTPPool
         , http_client_config := HTTPClientConfig
         , staged_files := StagedFiles
         } = TransferState,
        case do_insert_files_request(StagedFiles, HTTPPool, HTTPClientConfig) of
            {ok, 200, _, Body} ->
                {ok, emqx_utils_json:decode(Body, [return_maps])};
            Res ->
                exit({insert_failed, Res})
        end
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec create_action(connector_resource_id(), action_resource_id(), action_config(), connector_state()) ->
          {ok, action_state()} | {error, term()}.
create_action(ConnResId, ActionResId, #{parameters := #{mode := aggregated}} = ActionConfig, ConnState) ->
    maybe
        {ok, ActionState0} ?= start_http_pool(ActionResId, ActionConfig, ConnState),
        start_aggregator(ConnResId, ActionResId, ActionConfig, ActionState0)
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
    PipePrefix = iolist_to_binary([ <<"https://">>
                               , Host
                               , <<":">>
                               , integer_to_binary(Port)
                               , <<"/v1/data/pipes/">>
                               , PipePath
                               ]),
    InserFilesPath = iolist_to_binary([ PipePrefix
                                      , <<"/insertFiles">>
                                      ]),
    InserReportPath = iolist_to_binary([ PipePrefix
                                      , <<"/insertReport">>
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
                            , insert_report_path => InserReportPath
                            , max_retries => MaxRetries
                            , request_ttl => RequestTTL
                            }}};
        {error, Reason} ->
            {error, Reason}
    end.

start_aggregator(ConnResId, ActionResId, ActionConfig, ActionState0) ->
    'Elixir.IO':inspect(ActionConfig, [{label, config}]),
    maybe
        #{ bridge_name := Name
         , parameters := #{
                           mode := aggregated = Mode,
                           database := Database,
                           schema := Schema,
                           stage := Stage,
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
        odbc_pool => ConnResId,
      database => Database,
      schema => Schema,
      stage => Stage,
        http_pool => ActionResId,
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
            _ = ehttpc_sup:stop_pool(ActionResId),
            {error, Reason}
    end.

-spec destroy_action(action_resource_id(), action_state()) -> ok.
destroy_action(ActionResId, ActionState) ->
    case ActionState of
        #{on_stop := {M, F, A}} ->
            ok = apply(M, F, A);
        _ ->
            ok
    end,
    ok = ehttpc_sup:stop_pool(ActionResId),
    ok.

run_aggregated_action(Batch, #{aggreg_id := AggregId}) ->
    Timestamp = erlang:system_time(second),
    case emqx_connector_aggregator:push_records(AggregId, Timestamp, Batch) of
        ok ->
            ok;
        {error, Reason} ->
            {error, {unrecoverable_error, Reason}}
    end.

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

do_insert_files_request(StagedFiles, HTTPPool, HTTPClientConfig) ->
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
    Body = emqx_utils_json:encode(#{files => StagedFiles}),
    Req = {InserFilesPath, Headers, Body},
    Response = ehttpc:request(
                 HTTPPool,
                 post,
                 Req,
                 RequestTTL,
                 MaxRetries
                ),
    Response.

do_insert_report_request(HTTPPool, Opts, HTTPClientConfig) ->
    #{ jwt_config := JWTConfig
     , insert_report_path := InsertReportPath0
     , request_ttl := RequestTTL
     , max_retries := MaxRetries
     } = HTTPClientConfig,
    JWTToken = emqx_connector_jwt:ensure_jwt(JWTConfig),
    AuthnHeader = [<<"BEARER ">>, JWTToken],
    Headers = [ {<<"X-Snowflake-Authorization-Token-Type">>, <<"KEYPAIR_JWT">>}
              , {<<"Content-Type">>, <<"application/json">>}
              , {<<"Authorization">>, AuthnHeader}
              ],
    InsertReportPath = case Opts of
                           #{begin_mark := BeginMark} when is_binary(BeginMark) ->
                               <<InsertReportPath0/binary, "?beginMark=", BeginMark/binary>>;
                           _ ->
                               InsertReportPath0
                       end,
    Req = {InsertReportPath, Headers},
    Response = ehttpc:request(
                 HTTPPool,
                 get,
                 Req,
                 RequestTTL,
                 MaxRetries
                ),
    case Response of
        {ok, 200, _Headers, Body0} ->
            Body = emqx_utils_json:decode(Body0, [return_maps]),
            {ok, Body};
        _ ->
            {error, Response}
    end.

row_to_map(Row0, Headers) ->
    Row1 = tuple_to_list(Row0),
    Row2 = lists:map(fun emqx_utils_conv:bin/1, Row1),
    Row = lists:zip(Headers, Row2),
    maps:from_list(Row).

action_status(#{mode := aggregated} = ActionState) ->
    #{aggreg_id := AggregId} = ActionState,
    %% NOTE: This will effectively trigger uploads of buffers yet to be uploaded.
    Timestamp = erlang:system_time(second),
    ok = emqx_connector_aggregator:tick(AggregId, Timestamp),
    ?status_connected.
