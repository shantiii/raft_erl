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
    Cluster = [{local, agent1}, {local, agent2}, {local, agent3}],
    lists:foreach(fun(Name) -> {ok, _Pid} = gen_statem:start_link(Name, raft_agent, [{cluster, Cluster}], [{debug, [trace,log]}]) end, Cluster),
	io:format("Args: ~p~n", [Args]),
	io:format("lol: ~p~n", [Omg]),
    timer:sleep(2000),
	erlang:halt(0).

%%====================================================================
%% Internal functions
%%====================================================================
