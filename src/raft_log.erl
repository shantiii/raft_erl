-module(raft_log).

-export([new/1, last_index/1, commit_index/1, applied_index/1, commit/2, sublist/2, op/2]).
-export([append/3, append_all/3, drop_after/2]).
-export([at/2, term_at/2, op_at/2]).

-record(state, {oplist=[], state=[], applied_idx=0, commit_idx=0}).
-record(op, {f=fun(X)->X end, args=[]}).

new(Init) -> #state{oplist=[{0, op(fun first_op/2, [Init])}]}.

op(Fun, Args) ->
    #op{f=Fun, args=Args}.

last_index(#state{oplist=Log}) -> length(Log).

applied_index(#state{applied_idx=Idx}) -> Idx.

commit_index(#state{commit_idx=Idx}) -> Idx.

commit(#state{oplist=Ops,state=State,commit_idx=OldIndex}=Log, NewIndex) when NewIndex > OldIndex ->
    io:format("COMMITTING~n"),
    % foreach operation between NewIndex and OldIndex, apply that operation to state
    ToApply = lists:sublist(Ops, OldIndex, NewIndex - OldIndex),
    NewState = lists:foldl(fun({Op, Args}, Acc) -> apply(Op, [Acc|Args]) end, State, ToApply),
    Log#state{state=NewState, commit_idx=NewIndex};

% Ignore NewIndex <= OldIndex
commit(Log, NewIndex) ->
%    io:format("NOT COMMITTING~p~n Index:(~p)~n State:(~p)~n Log:(~p)~n", [self(), NewIndex, Log#state.state, Log#state.oplist]),
    Log.

sublist(Log, Index) when Index >= length(Log#state.oplist) ->
    [];
sublist(Log, Index) ->
    lists:nthtail(Index, Log#state.oplist).

drop_after(Log, Index) ->
    lists:sublist(Log#state.oplist, Index).

append(#state{oplist=Log}=State, Term, Op) ->
    State#state{oplist=Log ++ [{Term, Op}]}.

append_all(State, _Term, []) ->
    State;
append_all(State, Term, [Op|Tail]) ->
    append(State, Term, Op).

%% Lookup commands

at(#state{oplist=Log}, Index) ->
    lists:nth(Index, Log).

term_at(Log, Index) ->
    {Term, _Op} = at(Log, Index),
    Term.

op_at(Log, Index) ->
    {_Term, Op} = at(Log, Index),
    Op.

%%

first_op(_, InitialState) -> InitialState.
