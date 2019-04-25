-module(raft_log).

-export([new/1, last_index/1]).
-export([at/2, term_at/2, op_at/2]).

-record(state, {oplist=[], state=[], applied_idx=0}).
-record(op, {f=fun(X)->X end, args=[]}).

new(Init) -> #state{oplist=[{0, op(fun first_op/2, [Init])}]}.

op(Fun, Args) ->
    #op{f=Fun, args=Args}.

last_index(#state{oplist=Log}) -> length(Log).

append(#state{oplist=Log}=State, Term, Op) ->
    State#state{oplist=Log ++ [{Term, Op}]}.

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
