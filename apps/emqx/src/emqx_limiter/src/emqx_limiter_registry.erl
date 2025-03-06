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

%% @doc Registry for limiter groups.
%% NOTE
%% This module is not designed to be used outside of the `emqx_limiter` application.

-module(emqx_limiter_registry).

-feature(maybe_expr, enable).

-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    register_group/3,
    register_group_kind_defaults/3,
    unregister_group/1,
    unregister_group_kind_defaults/1,
    find_group/1,
    list_groups/0,
    list_group_kinds_with_defaults/0,
    get_limiter_options/1
]).

%% `gen_server` callbacks

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-type group() :: emqx_limiter:group().
-type group_kind() :: emqx_limiter:group_kind().
-type name() :: emqx_limiter:name().
-type limiter_id() :: emqx_limiter:id().

-define(PT_KEY(GROUP), {?MODULE, GROUP}).
-define(DEFAULTS_PT_KEY(GROUP_KIND), {?MODULE, GROUP_KIND, fallback}).
-define(group_names, group_names).
-define(group_kinds_with_defaults, group_kinds_with_defaults).

-record(group, {
    name :: group(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()}
}).

-record(defaults, {
    module :: module(),
    limiter_options :: emqx_limiter:options()
}).

%%--------------------------------------------------------------------
%% gen_server messages
%%--------------------------------------------------------------------

-record(register_group, {
    group :: group(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()}
}).

-record(register_group_kind_defaults, {
    group_kind :: group_kind(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()}
}).

-record(unregister_group, {
    group :: group()
}).

-record(unregister_group_kind_defaults, {
    group_kind :: group_kind()
}).

-record(list_groups, {}).
-record(list_group_kinds_with_defaults, {}).

%%------------------------------------------------------------------------------
%% Internal API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_group(group(), module(), [{name(), emqx_limiter:options()}]) -> ok | no_return().
register_group({_GroupKind, _} = Group, Module, LimiterOptions) ->
    ok = assert_unique_names(LimiterOptions),
    case
        gen_server:call(
            ?MODULE,
            #register_group{
                group = Group,
                module = Module,
                limiter_options = maps:from_list(LimiterOptions)
            },
            infinity
        )
    of
        ok ->
            ok;
        {error, Reason} ->
            error(Reason)
    end.

-spec register_group_kind_defaults(group_kind(), module(), [{name(), emqx_limiter:options()}]) ->
    ok | no_return().
register_group_kind_defaults(GroupKind, Module, LimiterOptions) ->
    ok = assert_unique_names(LimiterOptions),
    case
        gen_server:call(
            ?MODULE,
            #register_group_kind_defaults{
                group_kind = GroupKind,
                module = Module,
                limiter_options = maps:from_list(LimiterOptions)
            },
            infinity
        )
    of
        ok ->
            ok;
        {error, Reason} ->
            error(Reason)
    end.

-spec unregister_group(group()) -> ok.
unregister_group({_GroupKind, _} = Group) ->
    gen_server:call(?MODULE, #unregister_group{group = Group}, infinity).

-spec unregister_group_kind_defaults(group_kind()) -> ok.
unregister_group_kind_defaults(GroupKind) ->
    gen_server:call(?MODULE, #unregister_group_kind_defaults{group_kind = GroupKind}, infinity).

-spec list_groups() -> [group()].
list_groups() ->
    gen_server:call(?MODULE, #list_groups{}, infinity).

-spec list_group_kinds_with_defaults() -> [group_kind()].
list_group_kinds_with_defaults() ->
    gen_server:call(?MODULE, #list_group_kinds_with_defaults{}, infinity).

-spec find_group(group()) -> {module(), [{name(), emqx_limiter:options()}]} | undefined.
find_group(Group) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        undefined ->
            undefined;
        #group{module = Module, limiter_options = LimiterOptions} ->
            {Module, maps:to_list(LimiterOptions)}
    end.

-spec get_limiter_options(limiter_id()) -> emqx_limiter:options() | no_return().
get_limiter_options({{GroupKind, _} = Group, Name} = LimiterId) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        #group{limiter_options = #{Name := LimiterOptions}} ->
            LimiterOptions;
        _ ->
            case persistent_term:get(?DEFAULTS_PT_KEY(GroupKind), undefined) of
                #defaults{limiter_options = #{Name := LimiterOptions}} ->
                    LimiterOptions;
                _ ->
                    error({limiter_not_found, LimiterId})
            end
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, #{
        ?group_names => sets:new([{version, 2}]),
        ?group_kinds_with_defaults => sets:new([{version, 2}])
    }}.

handle_call(
    #register_group{
        group = Group,
        module = Module,
        limiter_options = LimiterOptions
    } = Req,
    _From,
    #{?group_names := GroupNames} = State
) ->
    OldGroup = persistent_term:get(?PT_KEY(Group), undefined),
    case ensure_same_limiters(OldGroup, Req) of
        ok ->
            _ = persistent_term:put(?PT_KEY(Group), #group{
                name = Group, module = Module, limiter_options = LimiterOptions
            }),
            {reply, ok, State#{
                ?group_names := sets:add_element(Group, GroupNames)
            }};
        {error, _} = Error ->
            {reply, Error, State}
    end;
handle_call(
    #register_group_kind_defaults{
        group_kind = GroupKind,
        module = Module,
        limiter_options = LimiterOptions
    } = Req,
    _From,
    #{?group_kinds_with_defaults := GroupKinds} = State
) ->
    OldDefaults = persistent_term:get(?DEFAULTS_PT_KEY(GroupKind), undefined),
    case ensure_same_limiters(OldDefaults, Req) of
        ok ->
            _ = persistent_term:put(?DEFAULTS_PT_KEY(GroupKind), #defaults{
                module = Module,
                limiter_options = LimiterOptions
            }),
            {reply, ok, State#{
                ?group_kinds_with_defaults := sets:add_element(GroupKind, GroupKinds)
            }};
        {error, _} = Error ->
            {reply, Error, State}
    end;
handle_call(#unregister_group{group = Group}, _From, #{?group_names := GroupNames} = State) ->
    _ = persistent_term:erase(?PT_KEY(Group)),
    {reply, ok, State#{?group_names := sets:del_element(Group, GroupNames)}};
handle_call(
    #unregister_group_kind_defaults{group_kind = GroupKind},
    _From,
    #{?group_kinds_with_defaults := GroupKinds} = State
) ->
    _ = persistent_term:erase(?DEFAULTS_PT_KEY(GroupKind)),
    {reply, ok, State#{?group_kinds_with_defaults := sets:del_element(GroupKind, GroupKinds)}};
handle_call(#list_groups{}, _From, #{?group_names := GroupNames} = State) ->
    {reply, sets:to_list(GroupNames), State};
handle_call(
    #list_group_kinds_with_defaults{}, _From, #{?group_kinds_with_defaults := GroupKinds} = State
) ->
    {reply, sets:to_list(GroupKinds), State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignore, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Req}),
    {noreply, State}.

terminate(_Reason, State) ->
    #{
        ?group_names := GroupNames,
        ?group_kinds_with_defaults := GroupKinds
    } = State,
    lists:foreach(
        fun(Group) ->
            persistent_term:erase(?PT_KEY(Group))
        end,
        sets:to_list(GroupNames)
    ),
    lists:foreach(
        fun(GroupKind) ->
            persistent_term:erase(?DEFAULTS_PT_KEY(GroupKind))
        end,
        sets:to_list(GroupKinds)
    ),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_same_limiters(undefined = _OldGroup, _Req) ->
    ok;
ensure_same_limiters(#group{module = OldModule}, #register_group{module = NewModule}) when
    OldModule =/= NewModule
->
    {error, {different_limiter_modules, {old, OldModule}, {new, NewModule}}};
ensure_same_limiters(#defaults{module = OldModule}, #register_group_kind_defaults{
    module = NewModule
}) when
    OldModule =/= NewModule
->
    {error, {different_limiter_modules, {old, OldModule}, {new, NewModule}}};
ensure_same_limiters(#group{limiter_options = OldLimiterOptions}, #register_group{
    limiter_options = NewLimiterOptions
}) ->
    do_ensure_same_limiters(OldLimiterOptions, NewLimiterOptions);
ensure_same_limiters(#defaults{limiter_options = OldLimiterOptions}, #register_group_kind_defaults{
    limiter_options = NewLimiterOptions
}) ->
    do_ensure_same_limiters(OldLimiterOptions, NewLimiterOptions).

do_ensure_same_limiters(OldLimiterOptions, NewLimiterOptions) ->
    OldLimiterNames = lists:sort(maps:keys(OldLimiterOptions)),
    NewLimiterNames = lists:sort(maps:keys(NewLimiterOptions)),
    case OldLimiterNames =:= NewLimiterNames of
        true ->
            ok;
        false ->
            {error, {different_limiter_names, {old, OldLimiterNames}, {new, NewLimiterNames}}}
    end.

assert_unique_names(LimiterOptions) ->
    {Names, _} = lists:unzip(LimiterOptions),
    case Names -- lists:usort(Names) of
        [] ->
            ok;
        Duplicates ->
            error({duplicate_names, Duplicates})
    end.
