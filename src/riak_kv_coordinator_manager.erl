-module(riak_kv_coordinator_manager).

-behaviour(gen_server).

%% API
-export([start_link/0, cast/2, cast/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {coords}).

%% TODO: Remove or make whole (eg. crash safe)

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

cast(Id, Msg) ->
    gen_server:cast(?MODULE, {coord_cast, Id, Msg}).

cast(Node, Id, Msg) ->
    gen_server:cast({?MODULE,Node}, {coord_cast, Id, Msg}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{coords=[]}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({coord_cast, Id, Msg}, State) ->
    {Coord, State2} = get_coordinator(Id, State),
    gen_server:cast(Coord, Msg),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_coordinator(Id, State=#state{coords=Coords}) ->
    case orddict:find(Id, Coords) of
        {ok, Pid} ->
            {Pid, State};
        error ->
            {ok, Pid} = riak_kv_coordinator:start(Id),
            Coords2 = orddict:store(Id, Pid, Coords),
            State2 = State#state{coords=Coords2},
            {Pid, State2}
    end.
