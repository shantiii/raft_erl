-module(raft_agent).

-behavior(gen_statem).

%% g
%-export([start_link/0]).
%% gen_statem callbacks
-export([callback_mode/0, init/1]).
-export([follower/3, candidate/3]).

-record(state, {id,cluster=[]}).

%% gen_statem callbacks

callback_mode() -> [state_functions, state_enter].

init([{cluster, Cluster}]) ->
    {ok, follower, #state{cluster=Cluster}}.

%% Follower State
follower(enter, follower, #state{}) ->
    {keep_state_and_data, [action_reset_election()]};

follower(timeout, election, #state{}=Data) ->
    {next_state, candidate, Data}.
   
%% Candidate State
candidate(enter, follower, #state{}) ->
    {keep_state_and_data, [action_reset_election()]};
candidate(enter, candidate, #state{}) ->
    {keep_state_and_data, [action_reset_election()]};

candidate(timeout, election, #state{}) ->
    repeat_state_and_data.

%handle_event(rpc, election, candidate, #state{}=Data) ->
%   {repeat_state, Data, [{timeout, election_timeout(), election}]};

% min and max in millseconds
-define(election_min, 50).
-define(election_max, 200).
election_timeout() ->
   ?election_min + rand:uniform(?election_max-?election_min+1) - 1.

action_reset_election() ->
    {timeout, election_timeout(), election}.
