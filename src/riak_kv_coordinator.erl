-module(riak_kv_coordinator).

-behaviour(gen_server).

%% API
-export([start/1, cast/3, read/6, write/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id,
                epoch,
                seq,
                client}).

%%%===================================================================
%%% API
%%%===================================================================

%% start_link() ->
%%     gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start(Id) ->
    gen_server:start(?MODULE, [Id], []).

read(Node, ReqId, Me, Bucket, Key, Options) ->
    BKey = {Bucket, Key},
    From = {ReqId, Me},
    riak_kv_ensemble:cast(Node, BKey, {read, From, BKey, Options}).

write(Node, ReqId, Me, RO, Options) ->
    Bucket = riak_object:bucket(RO),
    Key = riak_object:key(RO),
    BKey = {Bucket, Key},
    From = {ReqId, Me},
    riak_kv_ensemble:cast(Node, BKey, {write, From, BKey, RO, Options}).

cast(Node, Id, Msg) ->
    riak_kv_coordinator_manager:cast(Node, Id, Msg).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Id]) ->
    {ok, C} = riak:local_client(),
    {ok, #state{id=Id,
                epoch=0,
                seq=0,
                client=C}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

descendant(O1, O2) ->
    %% (vclock:descends(O2#r_object.vclock,O1#r_object.vclock) == false
    V1 = riak_object:vclock(O1),
    V2 = riak_object:vclock(O2),
    vclock:descends(V1, V2).

handle_cast({write, {ReqId, Pid}, {Bucket, Key}, NewRO, _}, State=#state{client=C}) ->
    {Valid, Changed, RO2} = check_valid_and_merge(C, Bucket, Key, NewRO),
    case {Valid, Changed} of
        {true, _} ->
            {Result, _, State2} = maybe_write_back(true, NewRO, State),
            case Result of
                true ->
                    client_reply(ReqId, Pid, ok);
                _ ->
                    client_reply(ReqId, Pid, {error, unavailable})
            end,
            {noreply, State2};
        {false, true} ->
            {_, _, State2} = maybe_write_back(true, RO2, State),
            client_reply(ReqId, Pid, {error, value_changed}),
            {noreply, State2};
        _ ->
            client_reply(ReqId, Pid, {error, value_changed}),
            {noreply, State}
    end;

handle_cast({read, {ReqId, Pid}, {Bucket, Key}, _}, State=#state{client=C}) ->
    case C:get(Bucket, Key, [direct,{pr,quorum},{r,quorum},primary]) of
        {ok, RO} ->
            {Changed, RO2} = maybe_merge(RO),
            Old = old_epoch(RO2, State),
            case maybe_write_back(Changed or Old, RO2, State) of
                {false, _, State2} ->
                    client_reply(ReqId, Pid, {ok, RO2}),
                    {noreply, State2};
                {true, RO3, State2} ->
                    client_reply(ReqId, Pid, {ok, RO3}),
                    {noreply, State2};
                {failed, _, State2} ->
                    client_reply(ReqId, Pid, {error, unavailable}),
                    {noreply, State2}
            end;
        {error, notfound} ->
            client_reply(ReqId, Pid, {error, notfound}),
            {noreply, State};
        _ ->
            client_reply(ReqId, Pid, {error, unavailable}),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    %% io:format("~p/~p/~p: ~p~n", [node(), State#state.id, self(), Msg]),
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

check_valid_and_merge(C, Bucket, Key, NewRO) ->
    case C:get(Bucket, Key, [direct,{pr,quorum},{r,quorum},primary]) of
        {ok, RO} ->
            {Changed, RO2} = maybe_merge(RO),
            Valid = descendant(NewRO, RO2),
            {Valid, Changed, RO2};
        {error, notfound} ->
            {true, false, undefined};
        _ ->
            {false, false, undefined}
    end.

maybe_merge(RO) ->
    merge(riak_object:value_count(RO), RO).

merge(1, RO) ->
    {false, RO};
merge(_, RO) ->
    [First|Rest] = riak_object:get_contents(RO),
    %% TODO: epoch + vclock are better resolution approach, no need to have seq-id
    Pick = lists:foldl(fun({Meta,Val}, {MetaX, ValX}) ->
                               Tag1 = dict:fetch(<<"rxid">>, Meta),
                               Tag2 = dict:fetch(<<"rxid">>, MetaX),
                               if Tag1 > Tag2 ->
                                       {Meta, Val};
                                  true ->
                                       {MetaX, ValX}
                               end
                       end, First, Rest),
    {NewMD, NewVal} = Pick,
    RO2 = riak_object:update_metadata(RO, NewMD),
    RO3 = riak_object:update_value(RO2, NewVal),
    {true, RO3}.

make_tag(Epoch, Seq) ->
    %% term_to_binary({Epoch, Seq}).
    %% list_to_binary(io_lib:format("~b_~b", [Epoch, Seq])).
    <<Epoch:64/integer, Seq:64/integer>>.
    
tag(MD, State=#state{epoch=Epoch, seq=Seq}) ->
    Tag = make_tag(Epoch, Seq),
    MD2 = dict:store(<<"rxid">>, Tag, MD),
    State2 = State#state{seq=Seq+1},
    {MD2, State2}.

maybe_write_back(false, RO, State) ->
    {false, RO, State};
maybe_write_back(true, RO, State) ->
    case write(RO, State) of
        {ok, RO2, State2} ->
            {true, RO2, State2};
        {_, _, State2} ->
            {failed, RO, State2}
    end.

write(RO, State=#state{client=C}) ->
    {MD, State2} = tag(riak_object:get_metadata(RO), State),
    RO2 = riak_object:update_metadata(RO, MD),
    Result = C:put(RO2, [direct,{pw,quorum},{w,quorum},primary]),
    {Result, RO2, State2}.

old_epoch(RO, #state{epoch=Epoch}) ->
    Meta = riak_object:get_metadata(RO),
    Tag = make_tag(Epoch, 0),
    case dict:find(<<"rxid">>, Meta) of
        {ok, T} when T >= Tag ->
            false;
        _ ->
            true
    end.

client_reply(ReqId, Pid, Reply) ->
    Msg = {ReqId, Reply},
    Pid ! Msg.

