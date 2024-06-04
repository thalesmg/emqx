%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1
]).

%% `minirest_trails' API
-export([
    api_schema/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(BIF_MOD_STR, "emqx_message_transformation_bif").

-type key() :: list(binary()) | binary().
-reflect_type([key/0]).

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() -> message_transformation.

roots() ->
    [{message_transformation, mk(ref(message_transformation), #{importance => ?IMPORTANCE_HIDDEN})}].

fields(message_transformation) ->
    [
        {transformations,
            mk(
                hoconsc:array(ref(transformation)),
                #{
                    default => [],
                    desc => ?DESC("validations"),
                    validator => fun validate_unique_names/1
                }
            )}
    ];
fields(transformation) ->
    [
        {tags, emqx_schema:tags_schema()},
        {description, emqx_schema:description_schema()},
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {name,
            mk(
                binary(),
                #{
                    required => true,
                    validator => fun emqx_resource:validate_name/1,
                    desc => ?DESC("name")
                }
            )},
        {topics,
            mk(
                hoconsc:union([binary(), hoconsc:array(binary())]),
                #{
                    desc => ?DESC("topics"),
                    converter => fun ensure_array/2,
                    validator => fun validate_unique_topics/1,
                    required => true
                }
            )},
        {failure_action,
            mk(
                hoconsc:enum([drop, disconnect, ignore]),
                #{desc => ?DESC("failure_action"), required => true}
            )},
        {log_failure,
            mk(
                ref(log_failure),
                #{desc => ?DESC("log_failure_at"), default => #{}}
            )},
        {operations,
            mk(
              hoconsc:array(ref(operation)),
              #{
                desc => ?DESC("operation"),
                required => true,
                validator => fun validate_non_empty/1
               }
             )}
    ];
fields(log_failure) ->
    [
        {level,
            mk(
                hoconsc:enum([error, warning, notice, info, debug, none]),
                #{desc => ?DESC("log_failure_at"), default => info}
            )}
    ];
fields(operation) ->
    [
      %% TODO: more strict check??
      %% TODO: check if root keys are allowed
      {key, mk(
              typerefl:alias("string", key()), #{
                           desc => ?DESC("operation_key"),
                           required => true,
                           converter => fun convert_key/2
                          })}
    , {value, mk(typerefl:alias("string", any()), #{
                                                   desc => ?DESC("operation_value"),
                                                   required => true,
                                                   converter => fun compile_variform/2
                                                  })}
    ].

%%------------------------------------------------------------------------------
%% `minirest_trails' API
%%------------------------------------------------------------------------------

api_schema(list) ->
    hoconsc:array(ref(validation));
api_schema(lookup) ->
    ref(validation);
api_schema(post) ->
    ref(validation);
api_schema(put) ->
    ref(validation).

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

ensure_array(undefined, _) -> undefined;
ensure_array(L, _) when is_list(L) -> L;
ensure_array(B, _) -> [B].

validate_unique_names(Validations0) ->
    Validations = emqx_utils_maps:binary_key_map(Validations0),
    do_validate_unique_names(Validations, #{}).

do_validate_unique_names(_Validations = [], _Acc) ->
    ok;
do_validate_unique_names([#{<<"name">> := Name} | _Rest], Acc) when is_map_key(Name, Acc) ->
    {error, <<"duplicated name: ", Name/binary>>};
do_validate_unique_names([#{<<"name">> := Name} | Rest], Acc) ->
    do_validate_unique_names(Rest, Acc#{Name => true}).

validate_unique_topics(Topics) ->
    Grouped = maps:groups_from_list(
        fun(T) -> T end,
        Topics
    ),
    DuplicatedMap = maps:filter(
        fun(_T, Ts) -> length(Ts) > 1 end,
        Grouped
    ),
    case maps:keys(DuplicatedMap) of
        [] ->
            ok;
        Duplicated ->
            Msg = iolist_to_binary([
                <<"duplicated topics: ">>,
                lists:join(", ", Duplicated)
            ]),
            {error, Msg}
    end.

validate_non_empty([]) ->
    {error, <<"must be non-empty">>};
validate_non_empty([_ | _]) ->
    ok.

compile_variform(Expression, #{make_serializable := true}) ->
    case is_binary(Expression) of
        true ->
            Expression;
        false ->
            emqx_variform:decompile(Expression)
    end;
compile_variform(Expression, _Opts) ->
    case emqx_variform:compile(Expression) of
        {ok, Compiled} ->
            transform_bifs(Compiled);
        {error, Reason} ->
            throw(#{expression => Expression, reason => Reason})
    end.

transform_bifs(#{form := Form} = Compiled) ->
    Compiled#{form := traverse_transform_bifs(Form)}.

traverse_transform_bifs({call, FnName, Args}) ->
    FQFnName = fully_qualify_local_bif(FnName),
    {call, FQFnName, lists:map(fun traverse_transform_bifs/1, Args)};
traverse_transform_bifs({array, Elems}) ->
    {array, lists:map(fun traverse_transform_bifs/1, Elems)};
traverse_transform_bifs(Node) ->
    Node.

fully_qualify_local_bif("schema_encode") ->
    ?BIF_MOD_STR ++ ".schema_encode";
fully_qualify_local_bif("schema_decode") ->
    ?BIF_MOD_STR ++ ".schema_decode";
fully_qualify_local_bif(FnName) ->
    FnName.

convert_key(X, _Opts) ->
    %% TODO: handle make_serializable
    logger:warning(#{x => X, o => _Opts}),
    case is_binary(X) of
        true ->
            [X];
        false ->
            X
    end.

parse_key_path(Key) when is_binary(Key) ->
    SegmentRE = <<"([^.\"']+|\"[^\"]+\"|'[^']+')">>,
    RE = [ <<"^">>
         , SegmentRE
         , [<<"(?:[.]">>, SegmentRE, <<")*">>]
         , <<"$">>
         ],
    case re:run(Key, RE, [{capture, first, binary}]) of
        nomatch ->
            throw(#{reason => invalid_key, key => Key});
        {match, Parts} ->

    end.
