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

handle_event(cast, {rpc, append_entries, Term, {LeaderId, _PrevLogIndex, _PrevLogTerm, [] = _Entries, _LeaderCommit}}, _Role, #state{id=Id, term=Term}) ->
    send(LeaderId, {ack, append_entries, Term, Id, true}),
    keep_state_and_data;

handle_event(cast, {rpc, append_entries, Term, {LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit}}, _Role, #state{id=Id, term=Term, log=Log}=Data) ->
    {Result, NewLog} =
    case raft_log:term_at(Log, PrevLogIndex) of
        PrevLogTerm ->
            {false, Log};
        _ ->
            Log1 = raft_log:drop_after(Log, PrevLogIndex),
            Log2 = raft_log:append_all(Log1, Entries),
            Log3 = raft_log:commit(Log2, min(LeaderCommit, raft_log:last_index(Log2))),
            {true, Log3}
    end,
    send(LeaderId, {ack, append_entries, Term, Id, Result}),
    {keep_state, Data#state{log=NewLog}};

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
    SendHeartbeat = fun(Server) -> send(Server, rpc(heartbeat, Data)) end,
    LastLogIndex = raft_log:last_index(Data#state.log),
    NextIndex = maps:from_list(lists:map(fun (Peer) -> {Peer, LastLogIndex + 1} end, Peers)),
    MatchIndex = maps:from_list(lists:map(fun (Peer) -> {Peer, 0} end, Peers)),
    lists:foreach(SendHeartbeat, Peers),
    {keep_state, Data#state{ldr_match_index=MatchIndex,ldr_next_index=NextIndex}, [action_heartbeat()]};

handle_event(cast, {client_req, Client, OpAndArgs}, leader, #state{term=Term,log=Log}=Data) -> 
    NewLog = raft_log:append(Log, Term, OpAndArgs),
    {keep_state, Data#state{log=NewLog}, [{next_event, internal, check_followers}]};

handle_event(internal, check_followers, leader, #state{cluster=Cluster,log=Log,ldr_next_index=NextIndices}=Data) ->
    LastIndex = raft_log:last_index(Log),
    AllPeers = peers(Cluster),
    CheckFn = fun(Peer) ->
                      #{Peer := NextIndex} = NextIndices,
                      LastIndex >= NextIndex
              end,
    OutOfDatePeers = lists:filter(CheckFn, AllPeers),
    SendAppendEntries = fun(Peer) -> send(Peer, rpc(append_entries, Peer, Data)) end,
    lists:foreach(SendAppendEntries, OutOfDatePeers),
    keep_state_and_data;

handle_event(cast, {ack, append_entries, Term, FollowerId, false}, leader, #state{ldr_next_index=NextIndices,log=Log}=Data) ->
    NewNextIndices = maps:update_with(FollowerId, fun(Value) -> Value-1 end, NextIndices),
    NewData = Data#state{ldr_next_index=NewNextIndices},
    send(FollowerId, rpc(append_entries, NewData)),
    {keep_state, NewData};

handle_event(cast, {ack, append_entries, Term, FollowerId, true}, leader, #state{ldr_match_index=MatchIndices, ldr_next_index=NextIndices,log=Log,cluster=Cluster}=Data) ->
    NewNextIndices = NextIndices#{FollowerId => raft_log:last_index(Log)},
    NewMatchIndices = MatchIndices#{FollowerId => raft_log:last_index(Log)},
    % if SOME INDEX is replicated to a majority of followers, commit that index (the highest SOME INDEX)
    % That index will be the median of the indices in NextIndices
    SortedIndices = lists:sort(maps:values(NewMatchIndices)),
    MedianIndex = length(Cluster) div 2,
    MajorityCommitted = lists:nth(MedianIndex + 1, SortedIndices),
    NewLog = raft_log:commit(Log, MajorityCommitted),
    {keep_state, Data#state{log=NewLog, ldr_next_index=NewNextIndices}};

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

rpc(append_entries, To, #state{ldr_next_index=NextIndices,term=Term,id=Id,log=Log}) ->
    #{To := NextIndex} = NextIndices,
    LastIndex = raft_log:last_index(Log),
    PrevLogIndex = NextIndex - 1,
    PrevLogTerm = raft_log:at(PrevLogIndex),
    Entries = raft_log:sublist(Log, NextIndex),
    {rpc, append_entries, Term, {Id, PrevLogIndex, PrevLogTerm, Entries, raft_log:commit_index(Log)}}.

rpc(heartbeat, #state{term=Term,id=Id,log=Log}) ->
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
