%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_psk).

-behaviour(emqx_authn_provider).

-include_lib("emqx/include/logger.hrl").

%% `emqx_authn_provider' API
-export([
   create/2,
   update/2,
   authenticate/2
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type config() :: #{}.
-type state() :: #{}.
-type credential() :: #{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------


%%------------------------------------------------------------------------------
%% `emqx_authn_provider' API
%%------------------------------------------------------------------------------

-spec create(emqx_authn_chains:authenticator_id(), config()) -> {ok, state()}.
create(_AuthenticatorId, _Config) ->
    State = #{step => init},
    {ok, State}.

-spec update(config(), state()) -> {ok, state()}.
update(_Config, State) ->
    {ok, State}.

-spec authenticate(credential(), state()) ->
          {continue, _AuthData, _AuthCache}
        | {ok, _Extra}.
authenticate(#{auth_cache := #{step := waiting_client_response} = State} = Credential, ResState) ->
    ?SLOG(warning, #{
                msg => "deleteme",
                cred => Credential,
                rst => ResState
            }),
    case Credential of
        #{auth_data := <<"boom">>} ->
            {ok, #{is_superuser => false}};
        _ ->
            {error, not_authorized}
    end;
authenticate(#{auth_cache := #{} = State} = Credential, ResState) ->
    ?SLOG(warning, #{
                msg => "deleteme",
                cred => Credential,
                     rst => ResState
            }),

    {continue, <<"hey">>, State#{step => waiting_client_response}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

rand_bytes() ->
    crypto:strong_rand_bytes(64).

stage1(#{state := init} = State0) ->
    Challenge = rand_bytes(),
    State = State0#{step := waiting_client_response},
    {continue, Challenge, State}.

stage2(#{state := waiting_client_response} = State0) ->
    State = State0#{step := done},
    {ok, allow, State}.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").



%% END ifdef(TEST).
-endif.
