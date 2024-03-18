%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_file_batcher_avro).

-behaviour(emqx_bridge_snowflake_file_batcher).

%% API
-export([]).

%% `emqx_bridge_snowflake_file_batcher' API
-export([init/1, add/2, finalize/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type batcher_opts() :: #{
    file := file:filename(),
    schema := emqx_schema:json_binary()
}.
-type batcher_state() :: #{
    encoder := avro:simple_encoder(),
    filename := file:filename(),
    fd := file:io_device(),
    header := avro_ocf:header(),
    type := avro:avro_type()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec init(batcher_opts()) -> {ok, batcher_state()} | {error, any()}.
init(Opts) ->
    #{
        file := Filename,
        schema := SchemaBin
    } = Opts,
    AvroType = avro:decode_schema(SchemaBin),
    Header = avro_ocf:make_header(AvroType),
    Encoder = avro:make_simple_encoder(SchemaBin, _Options = []),
    {ok, FD} = file:open(Filename, [write]),
    try
        ok = avro_ocf:write_header(FD, Header),
        State = #{
            encoder => Encoder,
            fd => FD,
            filename => Filename,
            header => Header,
            type => AvroType
        },
        {ok, State}
    catch
        Class:Reason ->
            file:close(FD),
            {error, {Class, Reason}}
    end.

add(State, Objects0) ->
    #{
        encoder := Encoder,
        fd := FD,
        header := Header
    } = State,
    Objects = lists:map(Encoder, Objects0),
    ok = avro_ocf:append_file(FD, Header, Objects),
    ok.

finalize(State) ->
    #{fd := FD, filename := Filename} = State,
    ok = file:close(FD),
    {ok, Filename}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
