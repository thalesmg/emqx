%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_echo_svr).

-behaviour(emqx_exproto_v_1_connection_handler_bhvr).

-export([
    start/0,
    stop/1
]).

-export([
    frame_connect/2,
    frame_connack/1,
    frame_publish/3,
    frame_puback/1,
    frame_subscribe/2,
    frame_suback/1,
    frame_unsubscribe/1,
    frame_unsuback/1,
    frame_disconnect/0
]).

-export([
    on_socket_created/2,
    on_received_bytes/2,
    on_socket_closed/2,
    on_timer_timeout/2,
    on_received_messages/2
]).

-define(LOG(Fmt, Args), ct:pal(Fmt, Args)).

-define(HTTP, #{
    grpc_opts => #{
        service_protos => [emqx_exproto_pb],
        services => #{'emqx.exproto.v1.ConnectionHandler' => ?MODULE}
    },
    listen_opts => #{
        port => 9001,
        socket_options => []
    },
    pool_opts => #{size => 8},
    transport_opts => #{ssl => false}
}).

-define(CLIENT, emqx_exproto_v_1_connection_adapter_client).

-define(send(Req), ?CLIENT:send(Req, #{channel => ct_test_channel})).
-define(close(Req), ?CLIENT:close(Req, #{channel => ct_test_channel})).
-define(authenticate(Req), ?CLIENT:authenticate(Req, #{channel => ct_test_channel})).
-define(start_timer(Req), ?CLIENT:start_timer(Req, #{channel => ct_test_channel})).
-define(publish(Req), ?CLIENT:publish(Req, #{channel => ct_test_channel})).
-define(subscribe(Req), ?CLIENT:subscribe(Req, #{channel => ct_test_channel})).
-define(unsubscribe(Req), ?CLIENT:unsubscribe(Req, #{channel => ct_test_channel})).

-define(TYPE_CONNECT, 1).
-define(TYPE_CONNACK, 2).
-define(TYPE_PUBLISH, 3).
-define(TYPE_PUBACK, 4).
-define(TYPE_SUBSCRIBE, 5).
-define(TYPE_SUBACK, 6).
-define(TYPE_UNSUBSCRIBE, 7).
-define(TYPE_UNSUBACK, 8).
-define(TYPE_DISCONNECT, 9).

-define(loop_recv_and_reply_empty_success(Stream),
    ?loop_recv_and_reply_empty_success(Stream, fun(_) -> ok end)
).

-define(loop_recv_and_reply_empty_success(Stream, Fun), begin
    LoopRecv = fun _Lp(_St) ->
        case grpc_stream:recv(_St) of
            {more, _Reqs, _NSt} ->
                ?LOG("~p: ~p~n", [?FUNCTION_NAME, _Reqs]),
                Fun(_Reqs),
                _Lp(_NSt);
            {eos, _Reqs, _NSt} ->
                ?LOG("~p: ~p~n", [?FUNCTION_NAME, _Reqs]),
                Fun(_Reqs),
                _NSt
        end
    end,
    NStream = LoopRecv(Stream),
    grpc_stream:reply(NStream, #{}),
    {ok, NStream}
end).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start() ->
    application:ensure_all_started(grpc),
    [start_channel(), start_server()].

start_channel() ->
    grpc_client_sup:create_channel_pool(ct_test_channel, "http://127.0.0.1:9100", #{}).

start_server() ->
    Services = #{
        protos => [emqx_exproto_pb],
        services => #{'emqx.exproto.v1.ConnectionHandler' => ?MODULE}
    },
    Options = [],
    grpc:start_server(?MODULE, 9001, Services, Options).

stop([_ChannPid, _SvrPid]) ->
    grpc:stop_server(?MODULE),
    grpc_client_sup:stop_channel_pool(ct_test_channel).

%%--------------------------------------------------------------------
%% Protocol Adapter callbacks
%%--------------------------------------------------------------------

-spec on_socket_created(grpc_stream:stream(), grpc:metadata()) ->
    {ok, grpc_stream:stream()}.
on_socket_created(Stream, _Md) ->
    ?loop_recv_and_reply_empty_success(Stream).

-spec on_socket_closed(grpc_stream:stream(), grpc:metadata()) ->
    {ok, grpc_stream:stream()}.
on_socket_closed(Stream, _Md) ->
    ?loop_recv_and_reply_empty_success(Stream).

-spec on_received_bytes(grpc_stream:stream(), grpc:metadata()) ->
    {ok, grpc_stream:stream()}.
on_received_bytes(Stream, _Md) ->
    ?loop_recv_and_reply_empty_success(
        Stream,
        fun(Reqs) ->
            lists:foreach(
                fun(#{conn := Conn, bytes := Bytes}) ->
                    #{<<"type">> := Type} = Params = emqx_json:decode(Bytes, [return_maps]),
                    _ = handle_in(Conn, Type, Params)
                end,
                Reqs
            )
        end
    ).

-spec on_timer_timeout(grpc_stream:stream(), grpc:metadata()) ->
    {ok, grpc_stream:stream()}.
on_timer_timeout(Stream, _Md) ->
    ?loop_recv_and_reply_empty_success(
        Stream,
        fun(Reqs) ->
            lists:foreach(
                fun(#{conn := Conn, type := 'KEEPALIVE'}) ->
                    ?LOG("Close this connection ~p due to keepalive timeout", [Conn]),
                    handle_out(Conn, ?TYPE_DISCONNECT),
                    ?close(#{conn => Conn})
                end,
                Reqs
            )
        end
    ).

-spec on_received_messages(grpc_stream:stream(), grpc:metadata()) ->
    {ok, grpc_stream:stream()}.
on_received_messages(Stream, _Md) ->
    ?loop_recv_and_reply_empty_success(
        Stream,
        fun(Reqs) ->
            lists:foreach(
                fun(#{conn := Conn, messages := Messages}) ->
                    lists:foreach(
                        fun(Message) ->
                            handle_out(Conn, ?TYPE_PUBLISH, Message)
                        end,
                        Messages
                    )
                end,
                Reqs
            )
        end
    ).

%%--------------------------------------------------------------------
%% The Protocol Example:
%%  CONN:
%%   {"type": 1, "clientinfo": {...}}
%%
%%  CONNACK:
%%   {"type": 2, "code": 0}
%%
%%  PUBLISH:
%%   {"type": 3, "topic": "xxx", "payload": "", "qos": 0}
%%
%%  PUBACK:
%%   {"type": 4, "code": 0}
%%
%%  SUBSCRIBE:
%%   {"type": 5, "topic": "xxx", "qos": 1}
%%
%%  SUBACK:
%%   {"type": 6, "code": 0}
%%
%%  DISCONNECT:
%%   {"type": 7, "code": 1}
%%--------------------------------------------------------------------

handle_in(Conn, ?TYPE_CONNECT, #{<<"clientinfo">> := ClientInfo, <<"password">> := Password}) ->
    NClientInfo = maps:from_list(
        [
            {binary_to_atom(K, utf8), V}
         || {K, V} <- maps:to_list(ClientInfo)
        ]
    ),
    case ?authenticate(#{conn => Conn, clientinfo => NClientInfo, password => Password}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            case maps:get(keepalive, NClientInfo, 0) of
                0 ->
                    ok;
                Intv ->
                    ?LOG("Try call start_timer with ~ps", [Intv]),
                    ?start_timer(#{conn => Conn, type => 'KEEPALIVE', interval => Intv})
            end,
            handle_out(Conn, ?TYPE_CONNACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_CONNACK, 1),
            ?close(#{conn => Conn})
    end;
handle_in(Conn, ?TYPE_PUBLISH, #{
    <<"topic">> := Topic,
    <<"qos">> := Qos,
    <<"payload">> := Payload
}) ->
    case ?publish(#{conn => Conn, topic => Topic, qos => Qos, payload => Payload}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            handle_out(Conn, ?TYPE_PUBACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_PUBACK, 1)
    end;
handle_in(Conn, ?TYPE_SUBSCRIBE, #{<<"qos">> := Qos, <<"topic">> := Topic}) ->
    case ?subscribe(#{conn => Conn, topic => Topic, qos => Qos}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            handle_out(Conn, ?TYPE_SUBACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_SUBACK, 1)
    end;
handle_in(Conn, ?TYPE_UNSUBSCRIBE, #{<<"topic">> := Topic}) ->
    case ?unsubscribe(#{conn => Conn, topic => Topic}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            handle_out(Conn, ?TYPE_UNSUBACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_UNSUBACK, 1)
    end;
handle_in(Conn, ?TYPE_DISCONNECT, _) ->
    ?close(#{conn => Conn}).

handle_out(Conn, ?TYPE_CONNACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_connack(Code)});
handle_out(Conn, ?TYPE_PUBACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_puback(Code)});
handle_out(Conn, ?TYPE_SUBACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_suback(Code)});
handle_out(Conn, ?TYPE_UNSUBACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_unsuback(Code)});
handle_out(Conn, ?TYPE_PUBLISH, #{qos := Qos, topic := Topic, payload := Payload}) ->
    ?send(#{conn => Conn, bytes => frame_publish(Topic, Qos, Payload)}).

handle_out(Conn, ?TYPE_DISCONNECT) ->
    ?send(#{conn => Conn, bytes => frame_disconnect()}).

%%--------------------------------------------------------------------
%% Frame

frame_connect(ClientInfo, Password) ->
    emqx_json:encode(#{
        type => ?TYPE_CONNECT,
        clientinfo => ClientInfo,
        password => Password
    }).
frame_connack(Code) ->
    emqx_json:encode(#{type => ?TYPE_CONNACK, code => Code}).

frame_publish(Topic, Qos, Payload) ->
    emqx_json:encode(#{
        type => ?TYPE_PUBLISH,
        topic => Topic,
        qos => Qos,
        payload => Payload
    }).

frame_puback(Code) ->
    emqx_json:encode(#{type => ?TYPE_PUBACK, code => Code}).

frame_subscribe(Topic, Qos) ->
    emqx_json:encode(#{type => ?TYPE_SUBSCRIBE, topic => Topic, qos => Qos}).

frame_suback(Code) ->
    emqx_json:encode(#{type => ?TYPE_SUBACK, code => Code}).

frame_unsubscribe(Topic) ->
    emqx_json:encode(#{type => ?TYPE_UNSUBSCRIBE, topic => Topic}).

frame_unsuback(Code) ->
    emqx_json:encode(#{type => ?TYPE_UNSUBACK, code => Code}).

frame_disconnect() ->
    emqx_json:encode(#{type => ?TYPE_DISCONNECT}).
