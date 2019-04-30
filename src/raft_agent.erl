-module(raft_agent).

-behavior(gen_statem).

%% g
%-export([start_link/0]).
%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4]).

%% Client API
%-export([query/2, update/2]).

% persisted state should be saved to disk after modification and before a callback returns
-record(state, {
          id,term=0,voted_for=[],log,cluster=[], %persisted state
          ldr_next_index, ldr_match_index, %volatile state
          votes_gathered=0
         }).

%% gen_statem callbacks

callback_mode() -> [handle_event_function, state_enter].

init(Args) ->
    State0 = parse_args(Args),
    State1 = State0#state{log=raft_log:new(#{})},
    {ok, follower, State1}.

%% Message Events that need to be handled:
%%
%% Election Timeout Elapsed (follower and candidate state only)
%% Received AppendEntries RPC
%% Received AppendEntries ACK
%% Received RequestVote RPC
%% Received RequestVote ACK
%% 
%% Received ClientRequest RPC -> Commit Transition / Query-NoOp
%% [Committed ClientEntry] -> Respond to Client

%% All States

%% Receiving an out-of-date RPC or ACK
%handle_event(cast, {_Type, Term, _Payload}=Msg, _Role, #state{term=CurrentTerm}=Data)
%  when Term > CurrentTerm ->
%    {next_state, follower, Data#state{term=Term, voted_for=[]}};

%% Follower State
handle_event(enter, follower, follower, #state{}) ->
    {keep_state_and_data, [action_reset_election()]};

handle_event(timeout, election, follower, #state{}=Data) ->
    {next_state, candidate, Data};
   
%% Candidate State
handle_event(enter, _PrevState, candidate, #state{term=Term,id=Id}=Data0) ->
    Data1 = Data0#state{term=Term+1, voted_for=Id, votes_gathered=1},
    lists:foreach(fun(Node) -> send(Node, rpc(request_vote, Data1)) end, peers(Data1)),
    {keep_state, Data1};


% if we are out of date, revert to follower
handle_event(cast, {rpc, _RpcType, Term, _Payload}=EventData, _State, #state{term=CurrentTerm}=Data) when Term > CurrentTerm ->
    {next_state, follower, Data#state{term=Term, voted_for=[]}, [{next_event, cast, EventData}]};

handle_event(cast, {rpc, request_vote, Term, {Candidate, _LastIdx, _LastTerm}}, _State, #state{term=CurrentTerm}) when Term < CurrentTerm ->
    send(Candidate, {ack, request_vote, CurrentTerm, false}),
    keep_state_and_data;

handle_event(cast, {rpc, request_vote, CurrentTerm, {Candidate, CLastIndex, CLastTerm}}, follower, #state{term=CurrentTerm,log=Log,voted_for=VotedFor}=Data) ->
    LastIndex = raft_log:last_index(Log),
    LastTerm = raft_log:term_at(Log, LastIndex),
    Result = 
    if
        not (VotedFor == [] orelse VotedFor == Candidate) ->
            false;
        LastTerm > CLastTerm ->
            false;
        LastIndex > CLastIndex ->
            false;
        true ->
            true
    end,
    send(Candidate, {ack, request_vote, CurrentTerm, Result}),
    case Result of
        true ->
            {keep_state, Data#state{voted_for=Candidate}};
        false ->
            keep_state_and_data
    end;

handle_event(cast, {rpc, append_entries, Term, {LeaderId, _PrevLogIndex, _PrevLogTerm, [] = _Entries, _LeaderCommit}}, _Role, #state{term=Term}) ->
    send(LeaderId, {ack, append_entries, Term, true}),
    keep_state_and_data;

handle_event(cast, {ack, append_entries, Term, true}, leader, #state{term=Term}) ->
    keep_state_and_data;

handle_event(cast, {ack, request_vote, Term, true}, candidate, #state{term=Term,votes_gathered=Votes,cluster=Cluster}=Data) ->
    NewVotes = Votes+1,
    NewData = Data#state{votes_gathered=NewVotes},
    if 
        NewVotes >= length(Cluster) div 2 ->
            {next_state, leader, NewData};
        true ->
            {keep_state, NewData}
    end;

%% Leader Mode!
handle_event(enter, leader, leader, Data) ->
    handle_event(enter, candidate, leader, Data);
handle_event(enter, candidate, leader, Data) ->
    % send to all members
    Peers = peers(Data),
    SendHeartbeat = fun(Server) -> send(Server, rpc(append_entries, Data)) end,
    lists:foreach(SendHeartbeat, Peers),
    {keep_state_and_data, [action_heartbeat()]};

%% Mostly Ignore votes we gather after being elected
handle_event(cast, {ack, request_vote, Term, Success}, leader, #state{term=Term,votes_gathered=Votes}=Data) ->
    case Success of
        true ->
            {keep_state, Data#state{votes_gathered=Votes+1}};
        false ->
            keep_state_and_data
    end;

handle_event(state_timeout, heartbeat, leader, #state{}) ->
    repeat_state_and_data;

handle_event(timeout, election, candidate, #state{}) ->
    repeat_state_and_data.

%% 
%% Private Functions
%%

%% A message

rpc(append_entries, #state{term=Term,id=Id,log=Log}) ->
    PrevLogIndex = 0,
    PrevLogTerm = 0,
    % TODO: populate entries
    Entries = [],
    {rpc, append_entries, Term, {Id, PrevLogIndex, PrevLogTerm, Entries, raft_log:commit_index(Log)}};
rpc(request_vote, #state{term=Term,id=Id,log=Log}) ->
    LastIndex = raft_log:last_index(Log),
    {rpc, request_vote, Term, {Id, LastIndex, raft_log:term_at(Log, LastIndex)}}.

% min and max in millseconds
-define(election_min, 150).
-define(election_max, 300).
election_timeout() ->
    ?election_min + rand:uniform(?election_max-?election_min+1) - 1.
heartbeat_timeout() ->
    50.

%% gen_statem action to handle event transition

action_reset_election() ->
    {timeout, election_timeout(), election}.

action_heartbeat() ->
    {state_timeout, heartbeat_timeout(), heartbeat}.

%action_retry_rpc(RpcData) ->
%    {state_timeout, heartbeat_timeout(), {retry, RpcData}}.

parse_args(Args) -> parse_args(Args, #state{}).

parse_args([{id, Id}|Rest], State) ->
    parse_args(Rest, State#state{id=Id});
parse_args([{cluster, Cluster}|Rest], State) ->
    parse_args(Rest, State#state{cluster=Cluster});
parse_args([], State) -> State.

peers(#state{id=Id,cluster=Cluster}) ->
    Cluster -- [{local, Id}].

%% MOCK

% TODO: Make this probabilistically fail
send(To, Data) when is_atom(To) ->
	io:format("Sending: ~p to ~p~n", [Data, To]),
    gen_statem:cast(To,Data);
send({local, To}, Data) ->
	io:format("Sending: ~p to ~p~n", [Data, To]),
    gen_statem:cast(To,Data).
