%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sub_topic_tree_index).

%% FIXME: just testing; deleteme
-compile([nowarn_export_all, export_all]).

%% API
-export([
    create_tables/0,

    register_hooks/0,
    unregister_hooks/0,

    on_session_subscribed/3,
    on_session_unsubscribed/3,

    inc_subscription/1,
    dec_subscription/1,

    next/2
]).

-include("emqx_mqtt.hrl").
-include("emqx_hooks.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(root, root).
-define(last_seen, last_seen).
-define(done, done).

-define(SUB_TOPIC_TREE_SHARD, emqx_sub_topic_tree_index_shard).
-define(SUB_TOPIC_TREE, emqx_sub_topic_tree_index).

-record(?SUB_TOPIC_TREE, {
   topic_filter,
   count
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

create_tables() ->
    ok= mria_config:set_dirty_shard(?SUB_TOPIC_TREE_SHARD, true),
    ok = mria:create_table(?SUB_TOPIC_TREE, [
        {type, ordered_set},
        {rlog_shard, ?SUB_TOPIC_TREE_SHARD},
        {storage, ram_copies},
        {record_name, ?SUB_TOPIC_TREE},
        {attributes, record_info(fields, ?SUB_TOPIC_TREE)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true},
                {decentralized_counters, true}
            ]}
        ]}
     ]),
    ok = mria:wait_for_tables([?SUB_TOPIC_TREE]),
    ok.

register_hooks() ->
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_HIGHEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_HIGHEST),
    ok.

unregister_hooks() ->
    ok = emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    ok = emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    ok.

on_session_subscribed(_ClientInfo, TopicFilter, _SubOpts) ->
    ok = inc_subscription(TopicFilter),
    ok.

on_session_unsubscribed(_ClientInfo, TopicFilter, _SubOpts) ->
    ok = dec_subscription(TopicFilter),
    ok.

inc_subscription(TopicFilter) when is_binary(TopicFilter) ->
    do_inc_subscription(TopicFilter);
inc_subscription(#share{topic = TopicFilter}) ->
    do_inc_subscription(TopicFilter).

dec_subscription(TopicFilter) when is_binary(TopicFilter) ->
    do_dec_subscription(TopicFilter);
dec_subscription(#share{topic = TopicFilter}) ->
    do_dec_subscription(TopicFilter).

next(Root, Opts) when is_binary(Root) ->
    LastSeen = maps:get(last_seen, Opts, undefined),
    BatchSize = maps:get(batch_size, Opts, 100),
    todo.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_inc_subscription(TopicFilter) when is_binary(TopicFilter) ->
    _ = mria:dirty_update_counter(?SUB_TOPIC_TREE, TopicFilter, 1),
    ok.

do_dec_subscription(TopicFilter) when is_binary(TopicFilter) ->
    case mria:dirty_update_counter(?SUB_TOPIC_TREE, TopicFilter, -1) of
        0 ->
            MS = erlang:make_tuple(
                   record_info(size, ?SUB_TOPIC_TREE),
                   '_',
                   [ {#?SUB_TOPIC_TREE.topic_filter, TopicFilter}
                   , {#?SUB_TOPIC_TREE.count, 0}
                   ]),
            _ = mria:match_delete(?SUB_TOPIC_TREE, MS),
            ok;
        _ ->
            ok
    end.

mk_iterator(Root, LastSeen) ->
    %% FIXME: remove root prefix from last seen
    #{ ?root => Root
     , ?last_seen => LastSeen
     }.

next(#{} = It0) ->
    #{?root := Root} = It0,
    RootSz = byte_size(Root),
    NextK = mk_key_for_next(It0),
    case mnesia:dirty_next(?SUB_TOPIC_TREE, NextK) of
        '$end_of_table' ->
            ?done;
        %% FIXME: must not expect "/" for Root = ""......
        <<Root:RootSz/binary, "/", Rest/binary>> = TopicFilter ->
            It = It0#{?last_seen := Rest},
            {ok, TopicFilter, It};
        _ ->
            ?done
    end.

mk_key_for_next(#{?root := <<"">>, ?last_seen := false}) ->
    <<"">>;
mk_key_for_next(#{?root := <<"">>, ?last_seen := LastSeen}) ->
    case binary:split(LastSeen, <<"/">>) of
        [Segment, _ | _] ->
            <<Segment/binary, 255>>;
        [Segment] ->
            Segment
    end;
mk_key_for_next(#{?root := Root, ?last_seen := false}) ->
    %% the predecessor of `/`: `$. = $/ - 1`, so we catch `Root/` (empty level).
    <<Root/binary, ".">>;
mk_key_for_next(#{?root := Root, ?last_seen := LastSeen}) ->
    case binary:split(LastSeen, <<"/">>) of
        [Segment, _ | _] ->
            <<Root/binary, "/", Segment/binary, 255>>;
        [Segment] ->
            <<Root/binary, "/", Segment>>
    end.
