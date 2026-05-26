%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_app).

-export([start/2]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_ds_sup:start_link(),
    emqx_bpapi:announce_new(node(), emqx_durable_storage),
    {ok, Sup}.
