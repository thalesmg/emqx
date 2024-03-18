%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_file_batcher).

-behaviour(gen_server).

%% API
-export([]).

%% `gen_server' API
-export([
    init/1,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type batcher_opts() :: map().
-type batcher_state() :: term().
-type data() :: term().

-type state() :: #{
    batcher := {module(), batcher_state()},
    flush_callback := fun((file:filename()) -> ok)
}.

%%------------------------------------------------------------------------------
%% Behaviour definition
%%------------------------------------------------------------------------------

-callback init(batcher_opts()) -> {ok, batcher_state()} | {error, any()}.
-callback add(batcher_state(), [data()]) -> ok | {error, any()}.
-callback finalize(batcher_state()) -> {ok, file:filename()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

-spec init(_) -> state().
init(Opts) ->
    #{
        batcher_mod := BatcherMod,
        batcher_opts := BatcherOpts,
        flush_callback := FlushCallback
    } = Opts,
    {ok, BatcherState} = BatcherMod:init(BatcherOpts),
    State = #{
        batcher => {BatcherMod, BatcherState},
        flush_callback => FlushCallback
    },
    {ok, State}.

handle_call(_Call, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({Ref, From, {append, Data}}, State) ->
    #{batcher := {Mod, BatcherState}} = State,
    ok = Mod:add(BatcherState, Data),
    From ! {Ref, From, ok},
    %% TODO: decide whether to flush or not, start timer, track number of messages and
    %% byte size, etc.
    {noreply, State};
handle_info(flush, State) ->
    %% Once the process decides to flush, it must terminate afterwards to avoid data
    %% duplication and data loss.  Any clients that sent messages after the batcher
    %% started to flush will retry targeting the new process that'll then be spawned.
    #{
        batcher := {Mod, BatcherState},
        flush_callback := FlushCallback
    } = State,
    {ok, Filename} = Mod:finalize(BatcherState),
    ok = FlushCallback(Filename),
    {stop, {shutdown, spindown}, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
