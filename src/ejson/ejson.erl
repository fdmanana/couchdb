-module(ejson).
-export([encode/1, decode/1, fuzz/0, fuzz/1]).
-on_load(init/0).

init() ->
    SoName = case code:priv_dir(ejson) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ejson]);
                _ ->
                    filename:join([priv, ejson])
            end;
        Dir ->
            filename:join(Dir, ejson)
    end,
    ok = erlang:load_nif(SoName, 0).

decode(IoList) ->
    case reverse_tokens(IoList) of
    {ok, ReverseTokens} ->
        [[EJson]] = make_ejson(ReverseTokens, [[]]),
        {ok, EJson};
    Error ->
        Error
    end.


encode(EJson) ->
    try
        RevList = encode_rev(EJson),
        {ok, final_encode(lists:reverse(lists:flatten([RevList])))}
    catch throw:Error ->
        Error
    end.

% Encode the json into a reverse list that's almost an iolist
% everything in the list is the final output except for tuples with
% {0, Strings} and {1, Floats}, which are to be converted to strings
% inside the NIF.
encode_rev(true) ->
    <<"true">>;
encode_rev(false) ->
    <<"false">>;
encode_rev(null) ->
    <<"null">>;
encode_rev(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
encode_rev(S) when is_binary(S) ->
    {0, S};
encode_rev(S) when is_atom(S) ->
    {0, list_to_binary(atom_to_list(S))};
encode_rev(F) when is_float(F) ->
    {1, F};
encode_rev({Props}) when is_list(Props) ->
    encode_proplist_rev(Props, [<<"{">>]);
encode_rev(Array) when is_list(Array) ->
    encode_array_rev(Array, [<<"[">>]);
encode_rev(Bad) ->
    throw({encode, {bad_term, Bad}}).


encode_array_rev([], Acc) ->
    [<<"]">> | Acc];
encode_array_rev([Val | Rest], [<<"[">>]) ->
    encode_array_rev(Rest, [encode_rev(Val), <<"[">>]);
encode_array_rev([Val | Rest], Acc) ->
    encode_array_rev(Rest, [encode_rev(Val), <<",">> | Acc]).


encode_proplist_rev([], Acc) ->
    [<<"}">> | Acc];
encode_proplist_rev([{Key,Val} | Rest], [<<"{">>]) ->
    encode_proplist_rev(
        Rest, [encode_rev(Val), <<":">>, {0, as_binary(Key)}, <<"{">>]);
encode_proplist_rev([{Key,Val} | Rest], Acc) ->
    encode_proplist_rev(
        Rest, [encode_rev(Val), <<":">>, {0, as_binary(Key)}, <<",">> | Acc]).

as_binary(B) when is_binary(B) ->
    B;
as_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A)).


make_ejson([], Stack) ->
    Stack;
make_ejson([0 | RevEvs], [ArrayValues, PrevValues | RestStack]) ->
    % 0 ArrayStart
    make_ejson(RevEvs, [[ArrayValues | PrevValues] | RestStack]);
make_ejson([1 | RevEvs], Stack) ->
    % 1 ArrayEnd
    make_ejson(RevEvs, [[] | Stack]);
make_ejson([2 | RevEvs], [ObjValues, PrevValues | RestStack]) ->
    % 2 ObjectStart
    make_ejson(RevEvs, [[{ObjValues} | PrevValues] | RestStack]);
make_ejson([3 | RevEvs], Stack) ->
    % 3 ObjectEnd
    make_ejson(RevEvs, [[] | Stack]);
make_ejson([{0, Value} | RevEvs], [Vals | RestStack] = _Stack) ->
    % {0, IntegerString}
    make_ejson(RevEvs, [[list_to_integer(binary_to_list(Value)) | Vals] | RestStack]);
make_ejson([{1, Value} | RevEvs], [Vals | RestStack] = _Stack) ->
    % {1, FloatString}
    make_ejson(RevEvs, [[list_to_float(binary_to_list(Value)) | Vals] | RestStack]);
make_ejson([{3, String} | RevEvs], [[PrevValue|RestObject] | RestStack] = _Stack) ->
    % {3 , ObjectKey}
    make_ejson(RevEvs, [[{String, PrevValue}|RestObject] | RestStack]);
make_ejson([Value | RevEvs], [Vals | RestStack] = _Stack) ->
    make_ejson(RevEvs, [[Value | Vals] | RestStack]).

fuzz() ->
    json_fuzz:fuzz(fun json_fuzz:choose/4).
fuzz(Chooser) ->
    json_fuzz:fuzz(Chooser).

not_loaded(Line) ->
    exit({ejson_not_loaded, module, ?MODULE, line, Line}).

reverse_tokens(_) ->
    not_loaded(?LINE).

final_encode(_) ->
    not_loaded(?LINE).
