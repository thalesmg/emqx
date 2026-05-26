%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_builtin_raft_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ok = emqx_dsch:register_backend(builtin_raft, emqx_ds_builtin_raft),
    {ok, Sup} = emqx_ds_builtin_raft_sup:start_top(),
    emqx_bpapi:announce_new(node(), emqx_ds_builtin_raft),
    {ok, Sup}.

stop(_) ->
    ok.
