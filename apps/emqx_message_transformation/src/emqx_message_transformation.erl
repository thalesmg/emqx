%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([
    add_handler/0,
    remove_handler/0,

    load/0,
    unload/0,

    list/0,
    reorder/1,
    lookup/1,
    insert/1,
    update/1,
    delete/1
]).

%% `emqx_hooks' API
-export([
    register_hooks/0,
    unregister_hooks/0,

    on_message_publish/1
]).

%% `emqx_config_handler' API
-export([pre_config_update/3, post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(TRACE_TAG, "MESSAGE_TRANSFORMATION").
-define(CONF_ROOT, message_transformation).
-define(CONF_ROOT_BIN, <<"message_transformation">>).
-define(TRANSFORMATIONS_CONF_PATH, [?CONF_ROOT, transformations]).

-type transformation_name() :: binary().
-type transformation() :: _TODO.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec add_handler() -> ok.
add_handler() ->
    ok = emqx_config_handler:add_handler([?CONF_ROOT], ?MODULE),
    ok = emqx_config_handler:add_handler(?TRANSFORMATIONS_CONF_PATH, ?MODULE),
    ok.

-spec remove_handler() -> ok.
remove_handler() ->
    ok = emqx_config_handler:remove_handler(?TRANSFORMATIONS_CONF_PATH),
    ok = emqx_config_handler:remove_handler([?CONF_ROOT]),
    ok.

load() ->
    Transformations = emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []),
    lists:foreach(
        fun({Pos, Transformation}) ->
            ok = emqx_message_transformation_registry:insert(Pos, Transformation)
        end,
        lists:enumerate(Transformations)
    ).

unload() ->
    Transformations = emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []),
    lists:foreach(
        fun(Transformation) ->
            ok = emqx_message_transformation_registry:delete(Transformation)
        end,
        Transformations
    ).

-spec list() -> [transformation()].
list() ->
    emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []).

-spec reorder([transformation_name()]) ->
    {ok, _} | {error, _}.
reorder(Order) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {reorder, Order},
        #{override_to => cluster}
    ).

-spec lookup(transformation_name()) -> {ok, transformation()} | {error, not_found}.
lookup(Name) ->
    Transformations = emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []),
    do_lookup(Name, Transformations).

-spec insert(transformation()) ->
    {ok, _} | {error, _}.
insert(Transformation) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {append, Transformation},
        #{override_to => cluster}
    ).

-spec update(transformation()) ->
    {ok, _} | {error, _}.
update(Transformation) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {update, Transformation},
        #{override_to => cluster}
    ).

-spec delete(transformation_name()) ->
    {ok, _} | {error, _}.
delete(Name) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {delete, Name},
        #{override_to => cluster}
    ).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_MESSAGE_TRANSFORMATION).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

-spec on_message_publish(emqx_types:message()) ->
    {ok, emqx_types:message()} | {stop, emqx_types:message()}.
on_message_publish(Message = #message{topic = Topic, headers = Headers}) ->
    case emqx_message_transformation_registry:matching_transformations(Topic) of
        [] ->
            ok;
        Transformations ->
            case run_transformations(Transformations, Message) of
                #message{} = FinalMessage ->
                    emqx_metrics:inc('messages.transformation_succeeded'),
                    {ok, FinalMessage};
                drop ->
                    emqx_metrics:inc('messages.transformation_failed'),
                    {stop, Message#message{headers = Headers#{allow_publish => false}}};
                disconnect ->
                    emqx_metrics:inc('messages.transformation_failed'),
                    {stop, Message#message{
                        headers = Headers#{
                            allow_publish => false,
                            should_disconnect => true
                        }
                    }}
            end
    end.

%%------------------------------------------------------------------------------
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

pre_config_update(?TRANSFORMATIONS_CONF_PATH, {append, Transformation}, OldTransformations) ->
    Transformations = OldTransformations ++ [Transformation],
    {ok, Transformations};
pre_config_update(?TRANSFORMATIONS_CONF_PATH, {update, Transformation}, OldTransformations) ->
    replace(OldTransformations, Transformation);
pre_config_update(?TRANSFORMATIONS_CONF_PATH, {delete, Transformation}, OldTransformations) ->
    delete(OldTransformations, Transformation);
pre_config_update(?TRANSFORMATIONS_CONF_PATH, {reorder, Order}, OldTransformations) ->
    reorder(OldTransformations, Order);
pre_config_update([?CONF_ROOT], {merge, NewConfig}, OldConfig) ->
    #{resulting_config := Config} = prepare_config_merge(NewConfig, OldConfig),
    {ok, Config};
pre_config_update([?CONF_ROOT], {replace, NewConfig}, _OldConfig) ->
    {ok, NewConfig}.

post_config_update(?TRANSFORMATIONS_CONF_PATH, {append, #{<<"name">> := Name}}, New, _Old, _AppEnvs) ->
    {Pos, Transformation} = fetch_with_index(New, Name),
    ok = emqx_message_transformation_registry:insert(Pos, Transformation),
    ok;
post_config_update(?TRANSFORMATIONS_CONF_PATH, {update, #{<<"name">> := Name}}, New, Old, _AppEnvs) ->
    {_Pos, OldTransformation} = fetch_with_index(Old, Name),
    {Pos, NewTransformation} = fetch_with_index(New, Name),
    ok = emqx_message_transformation_registry:update(OldTransformation, Pos, NewTransformation),
    ok;
post_config_update(?TRANSFORMATIONS_CONF_PATH, {delete, Name}, _New, Old, _AppEnvs) ->
    {_Pos, Transformation} = fetch_with_index(Old, Name),
    ok = emqx_message_transformation_registry:delete(Transformation),
    ok;
post_config_update(?TRANSFORMATIONS_CONF_PATH, {reorder, _Order}, New, _Old, _AppEnvs) ->
    ok = emqx_message_transformation_registry:reindex_positions(New),
    ok;
post_config_update([?CONF_ROOT], {merge, _}, ResultingConfig, Old, _AppEnvs) ->
    #{transformations := ResultingTransformations} = ResultingConfig,
    #{transformations := OldTransformations} = Old,
    #{added := NewTransformations0} =
        emqx_utils:diff_lists(
            ResultingTransformations,
            OldTransformations,
            fun(#{name := N}) -> N end
        ),
    NewTransformations =
        lists:map(
            fun(#{name := Name}) ->
                {Pos, Transformation} = fetch_with_index(ResultingTransformations, Name),
                ok = emqx_message_transformation_registry:insert(Pos, Transformation),
                #{name => Name, pos => Pos}
            end,
            NewTransformations0
        ),
    {ok, #{new_transformations => NewTransformations}};
post_config_update([?CONF_ROOT], {replace, Input}, ResultingConfig, Old, _AppEnvs) ->
    #{
        new_transformations := NewTransformations,
        changed_transformations := ChangedTransformations0,
        deleted_transformations := DeletedTransformations
    } = prepare_config_replace(Input, Old),
    #{transformations := ResultingTransformations} = ResultingConfig,
    #{transformations := OldTransformations} = Old,
    lists:foreach(
        fun(Name) ->
            {_Pos, Transformation} = fetch_with_index(OldTransformations, Name),
            ok = emqx_message_transformation_registry:delete(Transformation)
        end,
        DeletedTransformations
    ),
    lists:foreach(
        fun(Name) ->
            {Pos, Transformation} = fetch_with_index(ResultingTransformations, Name),
            ok = emqx_message_transformation_registry:insert(Pos, Transformation)
        end,
        NewTransformations
    ),
    ChangedTransformations =
        lists:map(
            fun(Name) ->
                {_Pos, OldTransformation} = fetch_with_index(OldTransformations, Name),
                {Pos, NewTransformation} = fetch_with_index(ResultingTransformations, Name),
                ok = emqx_message_transformation_registry:update(OldTransformation, Pos, NewTransformation),
                #{name => Name, pos => Pos}
            end,
            ChangedTransformations0
        ),
    ok = emqx_message_transformation_registry:reindex_positions(ResultingTransformations),
    {ok, #{changed_transformations => ChangedTransformations}}.

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(#{?CONF_ROOT_BIN := RawConf0}) ->
    Result = emqx_conf:update(
        [?CONF_ROOT],
        {merge, RawConf0},
        #{override_to => cluster, rawconf_with_defaults => true}
    ),
    case Result of
        {error, Reason} ->
            {error, #{root_key => ?CONF_ROOT, reason => Reason}};
        {ok, _} ->
            Keys0 = maps:keys(RawConf0),
            ChangedPaths = Keys0 -- [<<"transformations">>],
            {ok, #{root_key => ?CONF_ROOT, changed => ChangedPaths}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_ROOT, changed => []}}.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

evaluate_operation(Operation, Transformation, Message) ->
    #{name := _Name} = Transformation,
    #{key := K, value := V} = Operation,
    {Data, _} = emqx_rule_events:eventmsg_publish(Message),
    logger:warning(#{"ó" => emqx_variform:render(V, Data), k => K, v => V, d => Data}),
    {ok, Message}.

replace(OldTransformations, Transformation = #{<<"name">> := Name}) ->
    {Found, RevNewTransformations} =
        lists:foldl(
            fun
                (#{<<"name">> := NameIn}, {_FoundIn, Acc}) when NameIn =:= Name ->
                    {true, [Transformation | Acc]};
                (Val, {FoundIn, Acc}) ->
                    {FoundIn, [Val | Acc]}
            end,
            {false, []},
            OldTransformations
        ),
    case Found of
        true ->
            {ok, lists:reverse(RevNewTransformations)};
        false ->
            {error, not_found}
    end.

delete(OldTransformations, Name) ->
    {Found, RevNewTransformations} =
        lists:foldl(
            fun
                (#{<<"name">> := NameIn}, {_FoundIn, Acc}) when NameIn =:= Name ->
                    {true, Acc};
                (Val, {FoundIn, Acc}) ->
                    {FoundIn, [Val | Acc]}
            end,
            {false, []},
            OldTransformations
        ),
    case Found of
        true ->
            {ok, lists:reverse(RevNewTransformations)};
        false ->
            {error, not_found}
    end.

reorder(Transformations, Order) ->
    Context = #{
        not_found => sets:new([{version, 2}]),
        duplicated => sets:new([{version, 2}]),
        res => [],
        seen => sets:new([{version, 2}])
    },
    reorder(Transformations, Order, Context).

reorder(NotReordered, _Order = [], #{not_found := NotFound0, duplicated := Duplicated0, res := Res}) ->
    NotFound = sets:to_list(NotFound0),
    Duplicated = sets:to_list(Duplicated0),
    case {NotReordered, NotFound, Duplicated} of
        {[], [], []} ->
            {ok, lists:reverse(Res)};
        {_, _, _} ->
            Error = #{
                not_found => NotFound,
                duplicated => Duplicated,
                not_reordered => [N || #{<<"name">> := N} <- NotReordered]
            },
            {error, Error}
    end;
reorder(RemainingTransformations, [Name | Rest], Context0 = #{seen := Seen0}) ->
    case sets:is_element(Name, Seen0) of
        true ->
            Context = maps:update_with(
                duplicated, fun(S) -> sets:add_element(Name, S) end, Context0
            ),
            reorder(RemainingTransformations, Rest, Context);
        false ->
            case safe_take(Name, RemainingTransformations) of
                error ->
                    Context = maps:update_with(
                        not_found, fun(S) -> sets:add_element(Name, S) end, Context0
                    ),
                    reorder(RemainingTransformations, Rest, Context);
                {ok, {Transformation, Front, Rear}} ->
                    Context1 = maps:update_with(
                        seen, fun(S) -> sets:add_element(Name, S) end, Context0
                    ),
                    Context = maps:update_with(res, fun(Vs) -> [Transformation | Vs] end, Context1),
                    reorder(Front ++ Rear, Rest, Context)
            end
    end.

fetch_with_index([{Pos, #{name := Name} = Transformation} | _Rest], Name) ->
    {Pos, Transformation};
fetch_with_index([{_, _} | Rest], Name) ->
    fetch_with_index(Rest, Name);
fetch_with_index(Transformations, Name) ->
    fetch_with_index(lists:enumerate(Transformations), Name).

safe_take(Name, Transformations) ->
    case lists:splitwith(fun(#{<<"name">> := N}) -> N =/= Name end, Transformations) of
        {_Front, []} ->
            error;
        {Front, [Found | Rear]} ->
            {ok, {Found, Front, Rear}}
    end.

do_lookup(_Name, _Transformations = []) ->
    {error, not_found};
do_lookup(Name, [#{name := Name} = Transformation | _Rest]) ->
    {ok, Transformation};
do_lookup(Name, [_ | Rest]) ->
    do_lookup(Name, Rest).

run_transformations(Transformations, Message) ->
    try
        emqx_rule_runtime:clear_rule_payload(),
        Fun = fun(Transformation, Acc) ->
            #{name := Name} = Transformation,
            emqx_message_transformation_registry:inc_matched(Name),
            case run_transformation(Transformation, Acc) of
                #message{} = NewAcc ->
                    emqx_message_transformation_registry:inc_succeeded(Name),
                    {cont, NewAcc};
                ignore ->
                    trace_failure(Transformation, "transformation_failed", #{
                        transformation => Name,
                        action => ignore
                    }),
                    emqx_message_transformation_registry:inc_failed(Name),
                    run_message_transformation_failed_hook(Message, Transformation),
                    {cont, Acc};
                FailureAction ->
                    trace_failure(Transformation, "transformation_failed", #{
                        transformation => Name,
                        action => FailureAction
                    }),
                    emqx_message_transformation_registry:inc_failed(Name),
                    run_message_transformation_failed_hook(Message, Transformation),
                    {halt, FailureAction}
            end
        end,
        emqx_utils:foldl_while(Fun, Message, Transformations)
    after
        emqx_rule_runtime:clear_rule_payload()
    end.

run_transformation(Transformation, Message) ->
    #{
        operations := Operations,
        failure_action := FailureAction
    } = Transformation,
    Fun = fun(Transform, Acc) ->
        case evaluate_operation(Transform, Transformation, Acc) of
            {ok, NewAcc} -> {cont, NewAcc};
            error -> {halt, FailureAction}
        end
    end,
    emqx_utils:foldl_while(Fun, Message, Operations).

trace_failure(#{log_failure := #{level := none}} = Transformation, _Msg, _Meta) ->
    #{
        name := _Name,
        failure_action := _Action
    } = Transformation,
    ?tp(message_transformation_failed, #{log_level => none, name => _Name, action => _Action}),
    ok;
trace_failure(#{log_failure := #{level := Level}} = Transformation, Msg, Meta) ->
    #{
        name := _Name,
        failure_action := _Action
    } = Transformation,
    ?tp(message_transformation_failed, #{log_level => Level, name => _Name, action => _Action}),
    ?TRACE(Level, ?TRACE_TAG, Msg, Meta).

run_message_transformation_failed_hook(Message, Transformation) ->
    #{name := Name} = Transformation,
    TransformationContext = #{name => Name},
    emqx_hooks:run('message.transformation_failed', [Message, TransformationContext]).

%% "Merging" in the context of the transformation array means:
%%   * Existing transformations (identified by `name') are left untouched.
%%   * No transformations are removed.
%%   * New transformations are appended to the existing list.
%%   * Existing transformations are not reordered.
prepare_config_merge(NewConfig0, OldConfig) ->
    {ImportedRawTransformations, NewConfigNoTransformations} =
        case maps:take(<<"transformations">>, NewConfig0) of
            error ->
                {[], NewConfig0};
            {V, R} ->
                {V, R}
        end,
    OldRawTransformations = maps:get(<<"transformations">>, OldConfig, []),
    #{added := NewRawTransformations} = emqx_utils:diff_lists(
        ImportedRawTransformations,
        OldRawTransformations,
        fun(#{<<"name">> := N}) -> N end
    ),
    Config0 = emqx_utils_maps:deep_merge(OldConfig, NewConfigNoTransformations),
    Config = maps:update_with(
        <<"transformations">>,
        fun(OldVs) -> OldVs ++ NewRawTransformations end,
        NewRawTransformations,
        Config0
    ),
    #{
        new_transformations => NewRawTransformations,
        resulting_config => Config
    }.

prepare_config_replace(NewConfig, OldConfig) ->
    ImportedRawTransformations = maps:get(<<"transformations">>, NewConfig, []),
    OldTransformations = maps:get(transformations, OldConfig, []),
    %% Since, at this point, we have an input raw config but a parsed old config, we
    %% project both to the to have only their names, and consider common names as changed.
    #{
        added := NewTransformations,
        removed := DeletedTransformations,
        changed := ChangedTransformations0,
        identical := ChangedTransformations1
    } = emqx_utils:diff_lists(
        lists:map(fun(#{<<"name">> := N}) -> N end, ImportedRawTransformations),
        lists:map(fun(#{name := N}) -> N end, OldTransformations),
        fun(N) -> N end
    ),
    #{
        new_transformations => NewTransformations,
        changed_transformations => ChangedTransformations0 ++ ChangedTransformations1,
        deleted_transformations => DeletedTransformations
    }.
