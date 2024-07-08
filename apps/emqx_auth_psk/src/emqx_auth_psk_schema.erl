%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_psk_schema).

-behaviour(emqx_authn_schema).

%% `emqx_authn_schema' API
-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `emqx_authn_schema' API
%%------------------------------------------------------------------------------

namespace() -> "authn".

refs() -> [hoconsc:ref(?MODULE, psk_cr)].

select_union_member(#{<<"mechanism">> := <<"psk_cr">>}) ->
    refs();
select_union_member(_Value) ->
    undefined.

fields(psk_cr) ->
    [
     {mechanism, emqx_authn_schema:mechanism(psk_cr)},
     {backend, emqx_authn_schema:backend(password_based)}
    ] ++ emqx_authn_schema:common_fields().

desc(psk_cr) ->
    <<"todo">>;
desc(_) ->
    undefined.
