-module(riak_kv_ensemble).

-behaviour(gen_server).

%% API
-export([start_link/0, get_ensemble/1, all/0, cast/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-define(ETS, ensemble_ets).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_ensemble(Id) ->
    case ets:info(?ETS) of
        undefined ->
            gen_server:call(?MODULE, {get_ensemble, Id});
        _ ->
            hd(ets:lookup(?ETS, Id))
    end.

all() ->
    ets:tab2list(?ETS).

cast(Node, BKey, Msg) ->
    gen_server:cast({?MODULE,Node}, {ensemble_cast, BKey, Msg}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?ETS, [protected, named_table]),
    generate_ensembles(),
    {ok, #state{}}.

handle_call({get_ensemble, Id}, _From, State) ->
    [Reply] = ets:lookup(?ETS, Id),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ensemble_cast, BKey, Msg}, State) ->
    {Leader, Id} = key_ensemble(BKey),
    riak_kv_coordinator:cast(Leader, Id, Msg),
    {noreply, State};

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

generate_ensembles() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllPL = riak_core_ring:all_preflists(Ring, 3),
    Ensembles = [begin
                     {Indices, Nodes} = lists:unzip(PL),
                     N = length(Indices),
                     Leader = hd(Nodes),
                     First = hd(Indices),
                     {{First,N}, Leader, Indices, Nodes}
                 end || PL <- AllPL],
    ets:insert(?ETS, Ensembles).

key_ensemble(BKey={Bucket, _Key}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    Idx = riak_core_ring:responsible_index(DocIdx, Ring),
    Id = {Idx, N},
    [{_, Leader, _, _}] = ets:lookup(?ETS, Id),
    {Leader, Id}.
