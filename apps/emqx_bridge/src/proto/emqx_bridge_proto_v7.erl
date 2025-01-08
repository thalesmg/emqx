%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_proto_v7).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    %% removed in v7
    %% v2_list_bridges_on_nodes_v6/2,
    %% list_bridges_on_nodes/1,

    restart_bridge_to_node/3,
    start_bridge_to_node/3,
    stop_bridge_to_node/3,
    lookup_from_all_nodes/3,
    get_metrics_from_all_nodes/3,
    restart_bridges_to_all_nodes/3,
    start_bridges_to_all_nodes/3,
    stop_bridges_to_all_nodes/3,
    v2_lookup_from_all_nodes_v6/4,
    v2_get_metrics_from_all_nodes_v6/4,
    v2_start_bridge_on_node_v6/4,
    v2_start_bridge_on_all_nodes_v6/4,

    %% introduced in v7
    v2_list_summary_v7/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.8.5".

-type key() :: atom() | binary() | [byte()].

-spec restart_bridge_to_node(node(), key(), key()) ->
    term().
restart_bridge_to_node(Node, BridgeType, BridgeName) ->
    rpc:call(
        Node,
        emqx_bridge_resource,
        restart,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec start_bridge_to_node(node(), key(), key()) ->
    term().
start_bridge_to_node(Node, BridgeType, BridgeName) ->
    rpc:call(
        Node,
        emqx_bridge_resource,
        start,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec stop_bridge_to_node(node(), key(), key()) ->
    term().
stop_bridge_to_node(Node, BridgeType, BridgeName) ->
    rpc:call(
        Node,
        emqx_bridge_resource,
        stop,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec restart_bridges_to_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(ok).
restart_bridges_to_all_nodes(Nodes, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_resource,
        restart,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec start_bridges_to_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(ok).
start_bridges_to_all_nodes(Nodes, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_resource,
        start,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec stop_bridges_to_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(ok).
stop_bridges_to_all_nodes(Nodes, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_resource,
        stop,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec lookup_from_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
lookup_from_all_nodes(Nodes, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_api,
        lookup_from_local_node,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec get_metrics_from_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(emqx_metrics_worker:metrics()).
get_metrics_from_all_nodes(Nodes, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_api,
        get_metrics_from_local_node,
        [BridgeType, BridgeName],
        ?TIMEOUT
    ).

%% V2 Calls
-spec v2_lookup_from_all_nodes_v6([node()], emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
v2_lookup_from_all_nodes_v6(Nodes, ConfRootKey, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        lookup_from_local_node_v6,
        [ConfRootKey, BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec v2_get_metrics_from_all_nodes_v6([node()], emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
v2_get_metrics_from_all_nodes_v6(Nodes, ConfRootKey, ActionType, ActionName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        get_metrics_from_local_node_v6,
        [ConfRootKey, ActionType, ActionName],
        ?TIMEOUT
    ).

-spec v2_start_bridge_on_all_nodes_v6([node()], emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    emqx_rpc:erpc_multicall(ok).
v2_start_bridge_on_all_nodes_v6(Nodes, ConfRootKey, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2,
        start,
        [ConfRootKey, BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec v2_start_bridge_on_node_v6(node(), emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    term().
v2_start_bridge_on_node_v6(Node, ConfRootKey, BridgeType, BridgeName) ->
    rpc:call(
        Node,
        emqx_bridge_v2,
        start,
        [ConfRootKey, BridgeType, BridgeName],
        ?TIMEOUT
    ).

%%--------------------------------------------------------------------------------
%% introduced in v7
%%--------------------------------------------------------------------------------

-spec v2_list_summary_v7([node()], emqx_bridge_v2:root_cfg_key()) ->
    emqx_rpc:erpc_multicall([_FIXME]).
v2_list_summary_v7(Nodes, ConfRootKey) ->
    erpc:multicall(
        Nodes, emqx_bridge_v2_api, summary_from_local_node_v7, [ConfRootKey], ?TIMEOUT
    ).
