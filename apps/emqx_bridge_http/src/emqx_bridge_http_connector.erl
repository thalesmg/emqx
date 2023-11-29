%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_http_connector).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([reply_delegator/3]).

-export([
    roots/0,
    fields/1,
    desc/1,
    namespace/0
]).

%% for other http-like connectors.
-export([redact_request/1]).

-export([validate_method/1, join_paths/2]).

-define(DEFAULT_PIPELINE_SIZE, 100).
-define(DEFAULT_REQUEST_TIMEOUT_MS, 30_000).

-define(READACT_REQUEST_NOTE, "the request body is redacted due to security reasons").

%%=====================================================================
%% Hocon schema

namespace() -> "connector_http".

roots() ->
    fields(config).

fields(config) ->
    [
        {connect_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("connect_timeout")
                }
            )},
        {max_retries,
            sc(
                non_neg_integer(),
                #{deprecated => {since, "5.0.4"}}
            )},
        {retry_interval,
            sc(
                emqx_schema:timeout_duration(),
                #{deprecated => {since, "5.0.4"}}
            )},
        {pool_type,
            sc(
                hoconsc:enum([random, hash]),
                #{
                    default => random,
                    desc => ?DESC("pool_type")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )},
        {enable_pipelining,
            sc(
                pos_integer(),
                #{
                    default => ?DEFAULT_PIPELINE_SIZE,
                    desc => ?DESC("enable_pipelining")
                }
            )},
        {request,
            hoconsc:mk(
                ref("request"),
                #{
                    default => undefined,
                    required => false,
                    desc => ?DESC("request")
                }
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("request") ->
    [
        {method,
            hoconsc:mk(binary(), #{
                required => false,
                desc => ?DESC("method"),
                validator => fun ?MODULE:validate_method/1
            })},
        {path, hoconsc:mk(binary(), #{required => false, desc => ?DESC("path")})},
        {body, hoconsc:mk(binary(), #{required => false, desc => ?DESC("body")})},
        {headers, hoconsc:mk(map(), #{required => false, desc => ?DESC("headers")})},
        {max_retries,
            sc(
                non_neg_integer(),
                #{
                    required => false,
                    desc => ?DESC("max_retries")
                }
            )},
        {request_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    required => false,
                    desc => ?DESC("request_timeout")
                }
            )}
    ].

desc(config) ->
    "";
desc("request") ->
    "";
desc(_) ->
    undefined.

validate_method(M) when
    M =:= <<"post">>;
    M =:= <<"put">>;
    M =:= <<"get">>;
    M =:= <<"delete">>;
    M =:= post;
    M =:= put;
    M =:= get;
    M =:= delete
->
    ok;
validate_method(M) ->
    case string:find(M, "${") of
        nomatch ->
            {error,
                <<"Invalid method, should be one of 'post', 'put', 'get', 'delete' or variables in ${field} format.">>};
        _ ->
            ok
    end.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).

%% ===================================================================

callback_mode() -> async_if_possible.

on_start(
    InstId,
    #{
        base_url := #{
            scheme := Scheme,
            host := Host,
            port := Port,
            path := BasePath
        },
        connect_timeout := ConnectTimeout,
        pool_type := PoolType,
        pool_size := PoolSize
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_http_connector",
        connector => InstId,
        config => redact(Config)
    }),
    {Transport, TransportOpts} =
        case Scheme of
            http ->
                {tcp, []};
            https ->
                SSLOpts = emqx_tls_lib:to_client_opts(maps:get(ssl, Config)),
                {tls, SSLOpts}
        end,
    NTransportOpts = emqx_utils:ipv6_probe(TransportOpts),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30000},
        {pool_type, PoolType},
        {pool_size, PoolSize},
        {transport, Transport},
        {transport_opts, NTransportOpts},
        {enable_pipelining, maps:get(enable_pipelining, Config, ?DEFAULT_PIPELINE_SIZE)}
    ],
    State = #{
        pool_name => InstId,
        pool_type => PoolType,
        host => Host,
        port => Port,
        connect_timeout => ConnectTimeout,
        base_path => BasePath,
        request => preprocess_request(maps:get(request, Config, undefined))
    },
    case start_pool(InstId, PoolOpts) of
        ok ->
            case do_get_status(InstId, ConnectTimeout) of
                ok ->
                    {ok, State};
                Error ->
                    ok = ehttpc_sup:stop_pool(InstId),
                    Error
            end;
        Error ->
            Error
    end.

start_pool(PoolName, PoolOpts) ->
    case ehttpc_sup:start_pool(PoolName, PoolOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ?SLOG(warning, #{
                msg => "emqx_connector_on_start_already_started",
                pool_name => PoolName
            }),
            ok;
        Error ->
            Error
    end.

on_add_channel(
    _InstId,
    OldState,
    ActionId,
    ActionConfig
) ->
    InstalledActions = maps:get(installed_actions, OldState, #{}),
    {ok, ActionState} = do_create_http_action(ActionConfig),
    NewInstalledActions = maps:put(ActionId, ActionState, InstalledActions),
    NewState = maps:put(installed_actions, NewInstalledActions, OldState),
    {ok, NewState}.

do_create_http_action(_ActionConfig = #{parameters := Params}) ->
    {ok, preprocess_request(Params)}.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_http_connector",
        connector => InstId
    }),
    Res = ehttpc_sup:stop_pool(InstId),
    ?tp(emqx_connector_http_stopped, #{instance_id => InstId}),
    Res.

on_remove_channel(
    _InstId,
    OldState = #{installed_actions := InstalledActions},
    ActionId
) ->
    NewInstalledActions = maps:remove(ActionId, InstalledActions),
    NewState = maps:put(installed_actions, NewInstalledActions, OldState),
    {ok, NewState}.

%% BridgeV1 entrypoint
on_query(InstId, {send_message, Msg}, State) ->
    case maps:get(request, State, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        Request ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request(Request, Msg),
            %% bridge buffer worker has retry, do not let ehttpc retry
            Retry = 2,
            ClientId = maps:get(clientid, Msg, undefined),
            on_query(
                InstId,
                {ClientId, Method, {Path, Headers, Body}, Timeout, Retry},
                State
            )
    end;
%% BridgeV2 entrypoint
on_query(
    InstId,
    {ActionId, Msg},
    State = #{installed_actions := InstalledActions}
) when is_binary(ActionId) ->
    case {maps:get(request, State, undefined), maps:get(ActionId, InstalledActions, undefined)} of
        {undefined, _} ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        {_, undefined} ->
            ?SLOG(error, #{msg => "action_not_found", connector => InstId, action_id => ActionId}),
            {error, action_not_found};
        {Request, ActionState} ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request_and_action(Request, ActionState, Msg),
            %% bridge buffer worker has retry, do not let ehttpc retry
            Retry = 2,
            ClientId = maps:get(clientid, Msg, undefined),
            on_query(
                InstId,
                {ClientId, Method, {Path, Headers, Body}, Timeout, Retry},
                State
            )
    end;
on_query(InstId, {Method, Request}, State) ->
    %% TODO: Get retry from State
    on_query(InstId, {undefined, Method, Request, 5000, _Retry = 2}, State);
on_query(InstId, {Method, Request, Timeout}, State) ->
    %% TODO: Get retry from State
    on_query(InstId, {undefined, Method, Request, Timeout, _Retry = 2}, State);
on_query(
    InstId,
    {KeyOrNum, Method, Request, Timeout, Retry},
    #{base_path := BasePath} = State
) ->
    ?TRACE(
        "QUERY",
        "http_connector_received",
        #{
            request => redact_request(Request),
            note => ?READACT_REQUEST_NOTE,
            connector => InstId,
            state => redact(State)
        }
    ),
    NRequest = formalize_request(Method, BasePath, Request),
    Worker = resolve_pool_worker(State, KeyOrNum),
    Result0 = ehttpc:request(
        Worker,
        Method,
        NRequest,
        Timeout,
        Retry
    ),
    Result = transform_result(Result0),
    case Result of
        {error, {recoverable_error, Reason}} ->
            ?SLOG(warning, #{
                msg => "http_connector_do_request_failed",
                reason => Reason,
                connector => InstId
            }),
            {error, {recoverable_error, Reason}};
        {error, #{status_code := StatusCode}} ->
            ?SLOG(error, #{
                msg => "http_connector_do_request_received_error_response",
                note => ?READACT_REQUEST_NOTE,
                request => redact_request(NRequest),
                connector => InstId,
                status_code => StatusCode
            }),
            Result;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_connector_do_request_failed",
                note => ?READACT_REQUEST_NOTE,
                request => redact_request(NRequest),
                reason => Reason,
                connector => InstId
            }),
            Result;
        _Success ->
            Result
    end.

%% BridgeV1 entrypoint
on_query_async(InstId, {send_message, Msg}, ReplyFunAndArgs, State) ->
    case maps:get(request, State, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        Request ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request(Request, Msg),
            ClientId = maps:get(clientid, Msg, undefined),
            on_query_async(
                InstId,
                {ClientId, Method, {Path, Headers, Body}, Timeout},
                ReplyFunAndArgs,
                State
            )
    end;
%% BridgeV2 entrypoint
on_query_async(
    InstId,
    {ActionId, Msg},
    ReplyFunAndArgs,
    State = #{installed_actions := InstalledActions}
) when is_binary(ActionId) ->
    case {maps:get(request, State, undefined), maps:get(ActionId, InstalledActions, undefined)} of
        {undefined, _} ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        {_, undefined} ->
            ?SLOG(error, #{msg => "action_not_found", connector => InstId, action_id => ActionId}),
            {error, action_not_found};
        {Request, ActionState} ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request_and_action(Request, ActionState, Msg),
            ClientId = maps:get(clientid, Msg, undefined),
            on_query_async(
                InstId,
                {ClientId, Method, {Path, Headers, Body}, Timeout},
                ReplyFunAndArgs,
                State
            )
    end;
on_query_async(
    InstId,
    {KeyOrNum, Method, Request, Timeout},
    ReplyFunAndArgs,
    #{base_path := BasePath} = State
) ->
    Worker = resolve_pool_worker(State, KeyOrNum),
    ?TRACE(
        "QUERY_ASYNC",
        "http_connector_received",
        #{
            request => redact_request(Request),
            note => ?READACT_REQUEST_NOTE,
            connector => InstId,
            state => redact(State)
        }
    ),
    NRequest = formalize_request(Method, BasePath, Request),
    MaxAttempts = maps:get(max_attempts, State, 3),
    Context = #{
        attempt => 1,
        max_attempts => MaxAttempts,
        state => State,
        key_or_num => KeyOrNum,
        method => Method,
        request => NRequest,
        timeout => Timeout
    },
    ok = ehttpc:request_async(
        Worker,
        Method,
        NRequest,
        Timeout,
        {fun ?MODULE:reply_delegator/3, [Context, ReplyFunAndArgs]}
    ),
    {ok, Worker}.

resolve_pool_worker(State, undefined) ->
    resolve_pool_worker(State, self());
resolve_pool_worker(#{pool_name := PoolName} = State, Key) ->
    case maps:get(pool_type, State, random) of
        random ->
            ehttpc_pool:pick_worker(PoolName);
        hash ->
            ehttpc_pool:pick_worker(PoolName, Key)
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_get_status(_InstId, #{pool_name := PoolName, connect_timeout := Timeout} = State) ->
    case do_get_status(PoolName, Timeout) of
        ok ->
            connected;
        {error, still_connecting} ->
            connecting;
        {error, Reason} ->
            {disconnected, State, Reason}
    end.

do_get_status(PoolName, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(PoolName)],
    DoPerWorker =
        fun(Worker) ->
            case ehttpc:health_check(Worker, Timeout) of
                ok ->
                    ok;
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "http_connector_get_status_failed",
                        reason => redact(Reason),
                        worker => Worker
                    }),
                    Error
            end
        end,
    try emqx_utils:pmap(DoPerWorker, Workers, Timeout) of
        [] ->
            {error, still_connecting};
        [_ | _] = Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ok;
                Errors ->
                    hd(Errors)
            end
    catch
        exit:timeout ->
            ?SLOG(error, #{
                msg => "http_connector_pmap_failed",
                reason => timeout
            }),
            {error, timeout}
    end.

on_get_channel_status(
    InstId,
    _ChannelId,
    State
) ->
    %% XXX: Reuse the connector status
    on_get_status(InstId, State).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
preprocess_request(undefined) ->
    undefined;
preprocess_request(Req) when map_size(Req) == 0 ->
    undefined;
preprocess_request(
    #{
        method := Method,
        path := Path
    } = Req
) ->
    Headers = maps:get(headers, Req, []),
    #{
        method => parse_template(to_bin(Method)),
        path => parse_template(Path),
        body => maybe_parse_template(body, Req),
        headers => parse_headers(Headers),
        request_timeout => maps:get(request_timeout, Req, ?DEFAULT_REQUEST_TIMEOUT_MS),
        max_retries => maps:get(max_retries, Req, 2)
    }.

parse_headers(Headers) when is_map(Headers) ->
    maps:fold(
        fun(K, V, Acc) -> [parse_header(K, V) | Acc] end,
        [],
        Headers
    );
parse_headers(Headers) when is_list(Headers) ->
    lists:map(
        fun({K, V}) -> parse_header(K, V) end,
        Headers
    ).

parse_header(K, V) ->
    KStr = to_bin(K),
    VTpl = parse_template(to_bin(V)),
    {parse_template(KStr), maybe_wrap_auth_header(KStr, VTpl)}.

maybe_wrap_auth_header(Key, VTpl) when
    (byte_size(Key) =:= 19 orelse byte_size(Key) =:= 13)
->
    %% We check the size of potential keys in the guard above and consider only
    %% those that match the number of characters of either "Authorization" or
    %% "Proxy-Authorization".
    case try_bin_to_lower(Key) of
        <<"authorization">> ->
            emqx_secret:wrap(VTpl);
        <<"proxy-authorization">> ->
            emqx_secret:wrap(VTpl);
        _Other ->
            VTpl
    end;
maybe_wrap_auth_header(_Key, VTpl) ->
    VTpl.

try_bin_to_lower(Bin) ->
    try iolist_to_binary(string:lowercase(Bin)) of
        LowercaseBin -> LowercaseBin
    catch
        _:_ -> Bin
    end.

maybe_parse_template(Key, Conf) ->
    case maps:get(Key, Conf, undefined) of
        undefined -> undefined;
        Val -> parse_template(Val)
    end.

parse_template(String) ->
    emqx_template:parse(String).

process_request_and_action(Request, ActionState, Msg) ->
    MethodTemplate = maps:get(method, ActionState),
    Method = make_method(render_template_string(MethodTemplate, Msg)),
    BodyTemplate = maps:get(body, ActionState),
    Body = render_request_body(BodyTemplate, Msg),

    PathPrefix = unicode:characters_to_list(render_template(maps:get(path, Request), Msg)),
    PathSuffix = unicode:characters_to_list(render_template(maps:get(path, ActionState), Msg)),

    Path =
        case PathSuffix of
            "" -> PathPrefix;
            _ -> join_paths(PathPrefix, PathSuffix)
        end,

    HeadersTemplate1 = maps:get(headers, Request),
    HeadersTemplate2 = maps:get(headers, ActionState),
    Headers = merge_proplist(
        render_headers(HeadersTemplate1, Msg),
        render_headers(HeadersTemplate2, Msg)
    ),
    #{
        method => Method,
        path => Path,
        body => Body,
        headers => Headers,
        request_timeout => maps:get(request_timeout, ActionState)
    }.

merge_proplist(Proplist1, Proplist2) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            case lists:keyfind(K, 1, Acc) of
                false ->
                    [{K, V} | Acc];
                {K, _} = {K, V1} ->
                    [{K, V1} | Acc]
            end
        end,
        Proplist2,
        Proplist1
    ).

process_request(
    #{
        method := MethodTemplate,
        path := PathTemplate,
        body := BodyTemplate,
        headers := HeadersTemplate,
        request_timeout := ReqTimeout
    } = Conf,
    Msg
) ->
    Conf#{
        method => make_method(render_template_string(MethodTemplate, Msg)),
        path => unicode:characters_to_list(render_template(PathTemplate, Msg)),
        body => render_request_body(BodyTemplate, Msg),
        headers => render_headers(HeadersTemplate, Msg),
        request_timeout => ReqTimeout
    }.

render_request_body(undefined, Msg) ->
    emqx_utils_json:encode(Msg);
render_request_body(BodyTks, Msg) ->
    render_template(BodyTks, Msg).

render_headers(HeaderTks, Msg) ->
    lists:map(
        fun({K, V}) ->
            {
                render_template_string(K, Msg),
                render_template_string(emqx_secret:unwrap(V), Msg)
            }
        end,
        HeaderTks
    ).

render_template(Template, Msg) ->
    % NOTE: ignoring errors here, missing variables will be rendered as `"undefined"`.
    {String, _Errors} = emqx_template:render(Template, {emqx_jsonish, Msg}),
    String.

render_template_string(Template, Msg) ->
    unicode:characters_to_binary(render_template(Template, Msg)).

make_method(M) when M == <<"POST">>; M == <<"post">> -> post;
make_method(M) when M == <<"PUT">>; M == <<"put">> -> put;
make_method(M) when M == <<"GET">>; M == <<"get">> -> get;
make_method(M) when M == <<"DELETE">>; M == <<"delete">> -> delete.

formalize_request(Method, BasePath, {Path, Headers, _Body}) when
    Method =:= get; Method =:= delete
->
    formalize_request(Method, BasePath, {Path, Headers});
formalize_request(_Method, BasePath, {Path, Headers, Body}) ->
    {join_paths(BasePath, Path), Headers, Body};
formalize_request(_Method, BasePath, {Path, Headers}) ->
    {join_paths(BasePath, Path), Headers}.

%% By default, we cannot treat HTTP paths as "file" or "resource" paths,
%% because an HTTP server may handle paths like
%% "/a/b/c/", "/a/b/c" and "/a//b/c" differently.
%%
%% So we try to avoid unneccessary path normalization.
%%
%% See also: `join_paths_test_/0`
join_paths(Path1, Path2) ->
    do_join_paths(lists:reverse(to_list(Path1)), to_list(Path2)).

%% "abc/" + "/cde"
do_join_paths([$/ | Path1], [$/ | Path2]) ->
    lists:reverse(Path1) ++ [$/ | Path2];
%% "abc/" + "cde"
do_join_paths([$/ | Path1], Path2) ->
    lists:reverse(Path1) ++ [$/ | Path2];
%% "abc" + "/cde"
do_join_paths(Path1, [$/ | Path2]) ->
    lists:reverse(Path1) ++ [$/ | Path2];
%% "abc" + "cde"
do_join_paths(Path1, Path2) ->
    lists:reverse(Path1) ++ [$/ | Path2].

to_list(List) when is_list(List) -> List;
to_list(Bin) when is_binary(Bin) -> binary_to_list(Bin).

to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Str) when is_list(Str) ->
    list_to_binary(Str);
to_bin(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8).

reply_delegator(Context, ReplyFunAndArgs, Result0) ->
    spawn(fun() ->
        Result = transform_result(Result0),
        maybe_retry(Result, Context, ReplyFunAndArgs)
    end).

transform_result(Result) ->
    case Result of
        %% The normal reason happens when the HTTP connection times out before
        %% the request has been fully processed
        {error, Reason} when
            Reason =:= econnrefused;
            Reason =:= timeout;
            Reason =:= normal;
            Reason =:= {shutdown, normal};
            Reason =:= {shutdown, closed}
        ->
            {error, {recoverable_error, Reason}};
        {error, {closed, _Message} = Reason} ->
            %% _Message = "The connection was lost."
            {error, {recoverable_error, Reason}};
        {error, _Reason} ->
            Result;
        {ok, StatusCode, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            Result;
        {ok, StatusCode, _, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            Result;
        {ok, _TooManyRequests = StatusCode = 429, Headers} ->
            {error, {recoverable_error, #{status_code => StatusCode, headers => Headers}}};
        {ok, StatusCode, Headers} ->
            {error, {unrecoverable_error, #{status_code => StatusCode, headers => Headers}}};
        {ok, _TooManyRequests = StatusCode = 429, Headers, Body} ->
            {error,
                {recoverable_error, #{
                    status_code => StatusCode, headers => Headers, body => Body
                }}};
        {ok, StatusCode, Headers, Body} ->
            {error,
                {unrecoverable_error, #{
                    status_code => StatusCode, headers => Headers, body => Body
                }}}
    end.

maybe_retry(Result, _Context = #{attempt := N, max_attempts := Max}, ReplyFunAndArgs) when
    N >= Max
->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry(
    {error, {unrecoverable_error, #{status_code := _}}} = Result, _Context, ReplyFunAndArgs
) ->
    %% request was successful, but we got an error response; no need to retry
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry({error, Reason}, Context, ReplyFunAndArgs) ->
    #{
        state := State,
        attempt := Attempt,
        key_or_num := KeyOrNum,
        method := Method,
        request := Request,
        timeout := Timeout
    } = Context,
    %% TODO: reset the expiration time for free retries?
    IsFreeRetry =
        case Reason of
            {recoverable_error, normal} -> true;
            {recoverable_error, {shutdown, normal}} -> true;
            _ -> false
        end,
    NContext =
        case IsFreeRetry of
            true -> Context;
            false -> Context#{attempt := Attempt + 1}
        end,
    ?tp(http_will_retry_async, #{}),
    Worker = resolve_pool_worker(State, KeyOrNum),
    ok = ehttpc:request_async(
        Worker,
        Method,
        Request,
        Timeout,
        {fun ?MODULE:reply_delegator/3, [NContext, ReplyFunAndArgs]}
    ),
    ok;
maybe_retry(Result, _Context, ReplyFunAndArgs) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

%% The HOCON schema system may generate sensitive keys with this format
is_sensitive_key(Atom) when is_atom(Atom) ->
    is_sensitive_key(erlang:atom_to_binary(Atom));
is_sensitive_key(Bin) when is_binary(Bin), (size(Bin) =:= 19 orelse size(Bin) =:= 13) ->
    %% We want to convert this to lowercase since the http header fields
    %% are case insensitive, which means that a user of the Webhook bridge
    %% can write this field name in many different ways.
    case try_bin_to_lower(Bin) of
        <<"authorization">> -> true;
        <<"proxy-authorization">> -> true;
        _ -> false
    end;
is_sensitive_key(_) ->
    false.

%% Function that will do a deep traversal of Data and remove sensitive
%% information (i.e., passwords)
redact(Data) ->
    emqx_utils:redact(Data, fun is_sensitive_key/1).

%% because the body may contain some sensitive data
%% and at the same time the redact function will not scan the binary data
%% and we also can't know the body format and where the sensitive data will be
%% so the easy way to keep data security is redacted the whole body
redact_request({Path, Headers}) ->
    {Path, Headers};
redact_request({Path, Headers, _Body}) ->
    {Path, Headers, <<"******">>}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

redact_test_() ->
    TestData = #{
        headers => [
            {<<"content-type">>, <<"application/json">>},
            {<<"Authorization">>, <<"Basic YWxhZGRpbjpvcGVuc2VzYW1l">>}
        ]
    },
    [
        ?_assert(is_sensitive_key(<<"Authorization">>)),
        ?_assert(is_sensitive_key(<<"AuthoriZation">>)),
        ?_assert(is_sensitive_key('AuthoriZation')),
        ?_assert(is_sensitive_key(<<"PrOxy-authoRizaTion">>)),
        ?_assert(is_sensitive_key('PrOxy-authoRizaTion')),
        ?_assertNot(is_sensitive_key(<<"Something">>)),
        ?_assertNot(is_sensitive_key(89)),
        ?_assertNotEqual(TestData, redact(TestData))
    ].

join_paths_test_() ->
    [
        ?_assertEqual("abc/cde", join_paths("abc", "cde")),
        ?_assertEqual("abc/cde", join_paths("abc", "/cde")),
        ?_assertEqual("abc/cde", join_paths("abc/", "cde")),
        ?_assertEqual("abc/cde", join_paths("abc/", "/cde")),

        ?_assertEqual("/", join_paths("", "")),
        ?_assertEqual("/cde", join_paths("", "cde")),
        ?_assertEqual("/cde", join_paths("", "/cde")),
        ?_assertEqual("/cde", join_paths("/", "cde")),
        ?_assertEqual("/cde", join_paths("/", "/cde")),

        ?_assertEqual("//cde/", join_paths("/", "//cde/")),
        ?_assertEqual("abc///cde/", join_paths("abc//", "//cde/"))
    ].

-endif.
