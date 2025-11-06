%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-include_lib("emqx_auth/include/emqx_authz.hrl").

-define(ACL_SHARDED, emqx_acl_sharded).

%% To save some space, use an integer for label, 0 for 'all', {1, Username} and {2, ClientId}.
-define(ACL_TABLE_ALL, 0).
-define(ACL_TABLE_USERNAME, 1).
-define(ACL_TABLE_CLIENTID, 2).

-define(WHO(TYPE, NS), {NS, TYPE}).

-type username() :: {username, binary()}.
-type clientid() :: {clientid, binary()}.
-type who() :: username() | clientid() | all.

-type rule() :: {
    emqx_authz_rule:permission_resolution_precompile(),
    emqx_authz_rule:who_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

-type legacy_rule() :: {
    emqx_authz_rule:permission_resolution_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

-type rules() :: [rule() | legacy_rule()].

-type maybe_namespace() :: emqx_config:maybe_namespace().

%% Deprecated (since 6.1.0)
-record(?ACL_TABLE1, {
    who :: ?ACL_TABLE_ALL | {?ACL_TABLE_USERNAME, binary()} | {?ACL_TABLE_CLIENTID, binary()},
    rules :: rules()
}).

%% Introduced in 6.1.0
-record(?ACL_TABLE, {
    who ::
        ?WHO(?ACL_TABLE_ALL, maybe_namespace())
        | ?WHO({?ACL_TABLE_USERNAME, binary()}, maybe_namespace())
        | ?WHO({?ACL_TABLE_CLIENTID, binary()}, maybe_namespace()),
    rules :: rules(),
    extra = #{} :: map()
}).

-behaviour(emqx_authz_source).
-behaviour(emqx_db_backup).

%% AuthZ Callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    authorize/4
]).

%% Management API
-export([
    init_tables/0,
    store_rules/3,
    purge_rules/1,
    get_rules/2,
    delete_rules/2,
    list_clientid_rules/1,
    list_username_rules/1,
    record_count/1
]).

-export([backup_tables/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-spec create_tables() -> [mria:table()].
create_tables() ->
    ok = mria:create_table(?ACL_TABLE1, [
        {type, ordered_set},
        {rlog_shard, ?ACL_SHARDED},
        {storage, disc_copies},
        {attributes, record_info(fields, ?ACL_TABLE1)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    ok = mria:create_table(?ACL_TABLE, [
        {type, ordered_set},
        {rlog_shard, ?ACL_SHARDED},
        {storage, disc_copies},
        {attributes, record_info(fields, ?ACL_TABLE)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    [?ACL_TABLE, ?ACL_TABLE1].

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

create(Source) -> Source.

update(_State, Source) -> create(Source).

destroy(_Source) ->
    {atomic, ok} = mria:clear_table(?ACL_TABLE),
    ok.

authorize(
    #{
        username := Username,
        clientid := Clientid
    } = Client,
    PubSub,
    Topic,
    #{type := built_in_database}
) ->
    Namespace = get_namespace(Client),
    Rules =
        read_rules(?WHO({?ACL_TABLE_CLIENTID, Clientid}, Namespace)) ++
            read_rules(?WHO({?ACL_TABLE_USERNAME, Username}, Namespace)) ++
            read_rules(?WHO(?ACL_TABLE_ALL, Namespace)),
    do_authorize(Client, PubSub, Topic, Rules).

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> {<<"builtin_authz">>, [?ACL_TABLE]}.

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%% @doc Update authz rules
-spec store_rules(maybe_namespace(), who(), rules()) -> ok.
store_rules(Namespace, {username, Username}, Rules) ->
    do_store_rules(?WHO({?ACL_TABLE_USERNAME, Username}, Namespace), normalize_rules(Rules));
store_rules(Namespace, {clientid, Clientid}, Rules) ->
    do_store_rules(?WHO({?ACL_TABLE_CLIENTID, Clientid}, Namespace), normalize_rules(Rules));
store_rules(Namespace, all, Rules) ->
    do_store_rules(?WHO(?ACL_TABLE_ALL, Namespace), normalize_rules(Rules)).

%% @doc Clean all authz rules for (username & clientid & all)
-spec purge_rules(maybe_namespace()) -> ok.
purge_rules(Namespace) ->
    ok = lists:foreach(
        fun
            (?WHO(_, Ns) = Key) when Ns == Namespace ->
                ok = mria:dirty_delete(?ACL_TABLE, Key);
            (_Key) ->
                ok
        end,
        mnesia:dirty_all_keys(?ACL_TABLE)
    ).

%% @doc Get one record
-spec get_rules(maybe_namespace(), who()) -> {ok, rules()} | not_found.
get_rules(Namespace, {username, Username}) ->
    do_get_rules(?WHO({?ACL_TABLE_USERNAME, Username}, Namespace));
get_rules(Namespace, {clientid, Clientid}) ->
    do_get_rules(?WHO({?ACL_TABLE_CLIENTID, Clientid}, Namespace));
get_rules(Namespace, all) ->
    do_get_rules(?WHO(?ACL_TABLE_ALL, Namespace)).

%% @doc Delete one record
-spec delete_rules(maybe_namespace(), who()) -> ok.
delete_rules(Namespace, {username, Username}) ->
    mria:dirty_delete(?ACL_TABLE, ?WHO({?ACL_TABLE_USERNAME, Username}, Namespace));
delete_rules(Namespace, {clientid, Clientid}) ->
    mria:dirty_delete(?ACL_TABLE, ?WHO({?ACL_TABLE_CLIENTID, Clientid}, Namespace));
delete_rules(Namespace, all) ->
    mria:dirty_delete(?ACL_TABLE, ?WHO(?ACL_TABLE_ALL, Namespace)).

-spec list_username_rules(maybe_namespace()) -> ets:match_spec().
list_username_rules(Namespace) ->
    ets:fun2ms(
        fun(#?ACL_TABLE{who = ?WHO({?ACL_TABLE_USERNAME, Username}, Ns), rules = Rules}) when
            Ns == Namespace
        ->
            [{namespace, Ns}, {username, Username}, {rules, Rules}]
        end
    ).

-spec list_clientid_rules(maybe_namespace()) -> ets:match_spec().
list_clientid_rules(Namespace) ->
    ets:fun2ms(
        fun(#?ACL_TABLE{who = ?WHO({?ACL_TABLE_CLIENTID, Clientid}, Ns), rules = Rules}) when
            Ns == Namespace
        ->
            [{namespace, Ns}, {clientid, Clientid}, {rules, Rules}]
        end
    ).

-spec record_count(maybe_namespace()) -> non_neg_integer().
record_count(Namespace) ->
    MS = ets:fun2ms(fun(#?ACL_TABLE{who = ?WHO(_, Ns)}) when Ns == Namespace -> true end),
    ets:select_count(?ACL_TABLE, MS).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

read_rules(Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [] -> [];
        [#?ACL_TABLE{rules = Rules}] when is_list(Rules) -> Rules;
        Other -> error({invalid_rules, Key, Other})
    end.

do_store_rules(Who, Rules) ->
    Record = #?ACL_TABLE{who = Who, rules = Rules},
    mria:dirty_write(Record).

normalize_rules(Rules) ->
    lists:flatmap(fun normalize_rule/1, Rules).

normalize_rule(RuleRaw) ->
    case emqx_authz_rule_raw:parse_rule(RuleRaw) of
        %% For backward compatibility
        {ok, {Permission, Who, Action, TopicFilters}} ->
            [{Permission, Who, Action, TopicFilter} || TopicFilter <- TopicFilters];
        {error, Reason} ->
            error(Reason)
    end.

do_get_rules(Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [#?ACL_TABLE{rules = Rules}] -> {ok, Rules};
        [] -> not_found
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [Rule | Tail]) ->
    CompliledRule = compile_rule(Rule),
    case emqx_authz_rule:match(Client, PubSub, Topic, CompliledRule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

compile_rule({Permission, Who, Action, TopicFilter}) ->
    emqx_authz_rule:compile(Permission, Who, Action, [TopicFilter]);
compile_rule({Permission, Action, TopicFilter}) ->
    emqx_authz_rule:compile(Permission, all, Action, [TopicFilter]).

get_namespace(#{client_attrs := #{?CLIENT_ATTR_NAME_TNS := Namespace}}) when is_binary(Namespace) ->
    Namespace;
get_namespace(_ClientInfo) ->
    ?global_ns.
