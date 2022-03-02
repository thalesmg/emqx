#!/usr/bin/env escript

%% This script injects implicit relup instructions for emqx applications.

-mode(compile).

-define(ERROR(FORMAT, ARGS), io:format(standard_error, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).
-define(INFO(FORMAT, ARGS), io:format(user, "[inject-relup] " ++ FORMAT ++ "~n", ARGS)).

usage() ->
    "Usage: " ++ escript:script_name() ++ " <path-to-relup-file>".

main([RelupFile]) ->
    case filelib:is_regular(RelupFile) of
        true ->
            ok = inject_relup_file(RelupFile);
        false ->
            ?ERROR("not a valid file: ~p", [RelupFile]),
            erlang:halt(1)
    end;
main(_Args) ->
    ?ERROR("~s", [usage()]),
    erlang:halt(1).

inject_relup_file(File) ->
    case file:script(File) of
        {ok, {CurrRelVsn, UpVsnRUs, DnVsnRUs}} ->
            ?INFO("injecting instructions to: ~p", [File]),
            UpdatedContent = {CurrRelVsn,
                inject_relup_instrs(up, UpVsnRUs),
                inject_relup_instrs(down, DnVsnRUs)},
            file:write_file(File, term_to_text(UpdatedContent));
        {ok, _BadFormat} ->
            ?ERROR("bad formatted relup file: ~p", [File]),
            error({bad_relup_format, File});
        {error, enoent} ->
            ?INFO("relup file not found: ~p", [File]),
            ok;
        {error, Reason} ->
            ?ERROR("read relup file ~p failed: ~p", [File, Reason]),
            error({read_relup_error, Reason})
    end.

inject_relup_instrs(Type, RUs) ->
    lists:map(fun({Vsn, Desc, Instrs}) ->
        {Vsn, Desc, append_emqx_relup_instrs(Type, Vsn, Instrs)}
    end, RUs).

%% The `{apply, emqx_relup, post_release_upgrade, []}` will be appended to the end of
%% the instruction lists.
append_emqx_relup_instrs(up, FromRelVsn, Instrs0) ->
     {{UpExtra, _}, Instrs1} = filter_and_check_instrs(up, Instrs0),
     Instrs1 ++
        [ {load, {emqx_app, brutal_purge, soft_purge}}
        , {load, {emqx_relup, brutal_purge, soft_purge}}
        , {apply, {emqx_relup, post_release_upgrade, [FromRelVsn, UpExtra]}}
        ];

append_emqx_relup_instrs(down, ToRelVsn, Instrs0) ->
    {{_, DnExtra}, Instrs1} = filter_and_check_instrs(down, Instrs0)
    %% NOTE: When downgrading, we apply emqx_relup:post_release_downgrade/2 before reloading
    %%  or removing the emqx_relup module.
    Instrs1 ++
        [ {load, {emqx_app, brutal_purge, soft_purge}}
        , {apply, {emqx_relup, post_release_downgrade, [ToRelVsn, DnExtra]}}
        ],
    %% emqx_relup does not exist before release "4.4.2"
    LoadInsts =
        case ToRelVsn of
            ToRelVsn when ToRelVsn =:= "4.4.1"; ToRelVsn =:= "4.4.0" ->
                [{remove, {emqx_relup, brutal_purge, brutal_purge}}];
            _ ->
                [{load, {emqx_relup, brutal_purge, soft_purge}}]
        end,
    Instrs1 ++ LoadInsts.

filter_and_check_instrs(Type, Instrs) ->
    case filter_fetch_emqx_mods_and_extra(Instrs) of
        {_, _, [], _} ->
            ?ERROR("cannot find any 'load_object_code' instructions for app emqx", []),
            error({instruction_not_found, load_object_code});
        {UpExtra, DnExtra, EmqxMods, RemainInstrs} ->
            assert_mandatory_modules(Type, EmqxMods),
            {{UpExtra, DnExtra}, RemainInstrs}
    end.

filter_fetch_emqx_mods_and_extra(Instrs) ->
    lists:foldl(fun do_filter_and_get/2, {UpExtra, DnExtra, [], []}, Instrs).

%% collect modules for emqx app
do_filter_and_get({load_object_code, {emqx, _AppVsn, Mods}} = Instr,
        {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods ++ Mods, RemainInstrs ++ [Instr]};
%% remove 'load' instrs
do_filter_and_get({load, {Mod, _, _}}, {UpExtra, DnExtra, EmqxMods, RemainInstrs})
        when Mod =:= emqx_relup; Mod =:= emqx_app ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs};
%% remove 'remove' instrs
do_filter_and_get({remove, {emqx_relup, _, _}}, {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs};
%% remove 'apply' instrs for upgrade, and collect the 'Extra' parameter
do_filter_and_get({apply, {emqx_relup, post_release_upgrade, [_, UpExtra0]}},
        {_, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra0, DnExtra, EmqxMods, RemainInstrs};
%% remove 'apply' instrs for downgrade, and collect the 'Extra' parameter
do_filter_and_get({apply, {emqx_relup, post_release_downgrade, [_, DnExtra0]}},
        {UpExtra, _, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra0, EmqxMods, RemainInstrs};
%% keep all other instrs unchanged
do_filter_and_get(Instr, {UpExtra, DnExtra, EmqxMods, RemainInstrs}) ->
    {UpExtra, DnExtra, EmqxMods, RemainInstrs ++ [Instr]}.


assert_mandatory_modules(up, Mods) ->
    assert(lists:member(emqx_relup, Mods) andalso lists:member(emqx_app, Mods),
        "cannot find any 'load_object_code' instructions for emqx_app and emqx_rel: ~p", [Mods]);

assert_mandatory_modules(down, Mods) ->
    assert(lists:member(emqx_app, Mods),
        "cannot find any 'load_object_code' instructions for emqx_app", []).

assert(true, _, _) ->
    ok;
assert(false, Msg, Args) ->
    ?ERROR(Msg, Args),
    error(assert_failed).

term_to_text(Term) ->
    io_lib:format("~p.", [Term]).
