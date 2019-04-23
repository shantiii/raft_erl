-module(storage).
-behavior(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, terminate/2]).

%% public api
-export([start_link/0, load/1, store/2]).

init([]) ->
	TableRef = ets:new(storage, []),
	{ok, TableRef}.

terminate(_Reason, TableRef) ->
	catch ets:delete(TableRef),
	ok.

% TODO: deal with hard expectation to be running as 'storage' here

start_link() ->
	gen_server:start_link({local, storage}, ?MODULE, [], []).

load(Key) -> 
	gen_server:call(storage, {load, Key}).

store(Key, Value) -> 
	gen_server:call(storage, {store, Key, Value}).


%% private methods

handle_call({load, Key}, _From, TableRef) ->
	Reply = case ets:lookup(TableRef, Key) of
			[{Key, Value}] -> {ok, Value};
			[] -> error
		end,
	{reply, Reply, TableRef};

handle_call({store, Key, Value}, _From, TableRef) ->
	ets:insert(TableRef, {Key, Value}),
	{reply, ok, TableRef}.
