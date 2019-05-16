-module(raft_erl).

%% API exports
-export([main/1]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    storage:start_link(),
    storage:store(lol, wut),
    {ok, Omg} = storage:load(lol),
    %Debug = [],
    Debug = [{debug, [trace,log]}],
    %Cluster = [{local, agent1}],
    Cluster = [{local, agent1}, {local, agent2}, {local, agent3}],
    lists:foreach(
      fun({local, Id}=Name) ->
        {ok, _Pid} = gen_statem:start_link(Name, raft_agent, [{id, Id}, {cluster, Cluster}],
                                           Debug)
      end,
      Cluster),
    timer:sleep(1000),
    %io:format("Raft State: ~p~n", [raft_agent:operate(agent1, get_op())]),
    erlang:halt(0).

set_op(Value) ->
    raft_log:op(fun do_set/2, [Value]).

get_op() ->
    raft_log:op(fun do_get/1, []).

do_set(State, Value) ->
    {Value, ok}.

do_get(State) ->
    {State, State}.

%%====================================================================
%% Internal functions
%%====================================================================
