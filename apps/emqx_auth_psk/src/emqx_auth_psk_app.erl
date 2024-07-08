%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_psk_app).

-behaviour(application).

%% `application' API
-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `application' API
%%------------------------------------------------------------------------------

-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
    emqx_authn:register_provider({psk_cr, password_based}, emqx_auth_psk),
    {ok, Sup} = emqx_auth_psk_sup:start_link(),
    {ok, Sup}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok.
