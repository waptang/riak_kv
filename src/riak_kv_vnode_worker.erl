-module(riak_kv_vnode_worker).
-export([start_pool/0, start_link/1, vget/5, vput/7]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include_lib("riak_kv_vnode.hrl").

-define(DEFAULT_HASHTREE_TOKENS, 90).

-record(putargs, {returnbody :: boolean(),
                  coord:: boolean(),
                  lww :: boolean(),
                  bkey :: {binary(), binary()},
                  robj :: term(),
                  index_specs=[] :: [{index_op(), binary(), index_value()}],
                  reqid :: non_neg_integer(),
                  bprops :: maybe_improper_list(),
                  starttime :: non_neg_integer(),
                  prunetime :: undefined| non_neg_integer(),
                  is_index=false :: boolean() %% set if the b/end supports indexes
                 }).

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
%% -type index() :: non_neg_integer().

%%
%% Vnode worker process.  Lives for the duration of the request and
%% coordinates get/put requests against the backend.
%%

start_pool() ->
    PoolArgs = [{worker_module, ?MODULE},
                {size, 1},           %% TODO: Configure
                {max_overflow, 100}],%% TODO: Configure
    WorkerArgs = [],
    {ok, Pid} = poolboy:start_link(PoolArgs, WorkerArgs),
    register(?MODULE, Pid), % TOOD: Hacky, find something better.
    {ok, Pid}.

start_link([]) ->
    gen_server:start_link(?MODULE, [], []).

vget(GetReq, Sender, StartTS, Idx, BE) ->
    worker_req({vget, GetReq, Sender, StartTS, Idx, BE}).

vput(PutReq, Sender, StartTS, Idx, VId, BE, Trees) ->
    worker_req({vput, PutReq, Sender, StartTS, Idx, VId, BE, Trees}).

%% handoff_vput(PutReq, StartTS, Indx, VId, BE, Trees) ->
%%     worker_req({handoff_vput, PutReq, StartTS, Idx, VId, BE, Trees}).

worker_req(WorkerReq) ->
    WorkerTimeout = 100, %% TODO: Something better here
    case poolboy:checkout(?MODULE, true, WorkerTimeout) of
        Pid when is_pid(Pid) ->
            gen_server:cast(Pid, WorkerReq),
            Pid;
        Other ->
            Other
    end.

init([]) ->
    {ok, undefined}. %% No state needed yet.

handle_call(Msg, _From, State) ->
    {stop, normal, {bad_msg, Msg}, State}.

handle_cast({vget, ?KV_GET_REQ{bkey=BKey,
                               req_id=ReqId},
             Sender, StartTS, Idx, BE}, State) ->
    _WorkerStartTS = os:timestamp(),
    Reply = do_get(BKey, ReqId, Idx, BE),
    riak_core_vnode:reply(Sender, Reply),
    update_vnode_stats(vnode_get, Idx, StartTS),
    {stop, normal, State};
handle_cast({vput, ?KV_PUT_REQ{bkey=BKey,
                               object=Object,
                               req_id=ReqId,
                               start_time=StartTime,
                               options=Options},
             Sender, StartTS, Idx, VId, BE, Trees}, State) ->
    %% Get the object from the backend
    riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
    Reply = do_put(BKey,  Object, ReqId, StartTime, Options, Idx, VId, BE, Trees),
    riak_core_vnode:reply(Sender, Reply),
    update_vnode_stats(vnode_put, Idx, StartTS),
    {stop, normal, State}.
%% handle_cast({handoff_vput, ?KV_PUT_REQ{}, Sender, StartTS, Idx, BE, Trees}, State) ->
%%     _Reply = do_put(Sender, BKey,  Object, ReqId, StartTime, Options, VId, BE, Trees),
%%     %% TODO: Update handoff vput stats
%%     {stop, normal, State}.

handle_info(Msg, State) ->
    {stop, {bad_msg, Msg}, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


do_get(BKey, ReqID, Idx, BE) ->
    Retval = do_get_term(BKey, BE),
    {r, Retval, Idx, ReqID}.

%% @private
do_get_term(BKey, BE) ->
    case do_get_binary(BKey, BE) of
        {ok, Bin} ->
            {ok, object_from_binary(BKey, Bin)};
        %% @TODO Eventually it would be good to
        %% make the use of not_found or notfound
        %% consistent throughout the code.
        {error, not_found} ->
            {error, notfound};
        {error, Reason} ->
            {error, Reason}
    end.

do_get_binary({Bucket, Key}, BE) ->
    beget(BE, Bucket, Key).

%% upon receipt of a client-initiated put
do_put({Bucket,_Key}=BKey, RObj, ReqID, StartTime, Options, Idx, VId, BE, Trees) ->
    case proplists:get_value(bucket_props, Options) of
        undefined ->
            {ok,Ring} = riak_core_ring_manager:get_my_ring(),
            BProps = riak_core_bucket:get_bucket(Bucket, Ring);
        BProps ->
            BProps
    end,
    case proplists:get_value(rr, Options, false) of
        true ->
            PruneTime = undefined;
        false ->
            PruneTime = StartTime
    end,
    Coord = proplists:get_value(coord, Options, false),
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false) orelse Coord,
                       coord=Coord,
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       starttime=StartTime,
                       prunetime=PruneTime},
    {PrepPutRes, UpdPutArgs} = prepare_put(VId, BE, Idx, PutArgs),
    Reply = perform_put(PrepPutRes, BE, Idx, Trees, UpdPutArgs),
    update_index_write_stats(UpdPutArgs#putargs.is_index, UpdPutArgs#putargs.index_specs),
    Reply.


%% do_backend_delete(BKey, RObj, BE}) ->
%%     %% object is a tombstone or all siblings are tombstones
%%     %% Calculate the index specs to remove...
%%     %% JDM: This should just be a tombstone by this point, but better
%%     %% safe than sorry.
%%     IndexSpecs = riak_object:diff_index_specs(undefined, RObj),

%%     %% Do the delete...
%%     {Bucket, Key} = BKey,
%%     case Mod:delete(Bucket, Key, IndexSpecs, ModState) of
%%         {ok, UpdModState} ->
%%             riak_kv_index_hashtree:delete(BKey, State#state.hashtrees),
%%             update_index_delete_stats(IndexSpecs),
%%             State#state{modstate = UpdModState};
%%         {error, _Reason, UpdModState} ->
%%             State#state{modstate = UpdModState}
%%     end.

%% Compute a hash of the deleted object
%% delete_hash(RObj) ->
%%     erlang:phash2(RObj, 4294967296).

prepare_put(VId, BE = {BEMod, BECtx}, Idx,
            PutArgs=#putargs{bkey={Bucket, _Key},
                             lww=LWW,
                             coord=Coord,
                             robj=RObj,
                             starttime=StartTime}) ->
    %% Can we avoid reading the existing object? If this is not an
    %% index backend, and the bucket is set to last-write-wins, then
    %% no need to incur additional get. Otherwise, we need to read the
    %% old object to know how the indexes have changed.
    {ok, Capabilities} = BEMod:capabilities(Bucket, BECtx),
    IndexBackend = lists:member(indexes, Capabilities),
    case LWW andalso not IndexBackend of
        true ->
            ObjToStore =
                case Coord of
                    true ->
                        riak_object:increment_vclock(RObj, VId, StartTime);
                    false ->
                        RObj
                end,
            {{true, ObjToStore}, PutArgs#putargs{is_index = false}};
        false ->
            prepare_put(VId, BE, Idx, PutArgs, IndexBackend)
    end.

prepare_put(VId, BE, Idx,
            PutArgs=#putargs{bkey={Bucket, Key},
                             robj=RObj,
                             bprops=BProps,
                             coord=Coord,
                             lww=LWW,
                             starttime=StartTime,
                             prunetime=PruneTime},
            IndexBackend) ->
    GetReply =
        case beget(BE, Bucket, Key) of
            {error, not_found} ->
                ok;
            % NOTE: bad_crc is NOT an official backend response. It is
            % specific to bitcask currently and handling it may be changed soon.
            % A standard set of responses will be agreed on
            % https://github.com/basho/riak_kv/issues/496
            {error, bad_crc} ->
                lager:info("Bad CRC detected while reading Partition=~p, Bucket=~p, Key=~p", [Idx, Bucket, Key]),
                ok;
            {ok, GetVal} ->
                {ok, GetVal}
        end,
    case GetReply of
        ok ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(RObj);
                false ->
                    IndexSpecs = []
            end,
            ObjToStore = case Coord of
                             true ->
                                 riak_object:increment_vclock(RObj, VId, StartTime);
                             false ->
                                 RObj
                         end,
            {{true, ObjToStore}, PutArgs#putargs{index_specs=IndexSpecs, is_index=IndexBackend}};
        {ok, Val} ->
            OldObj = object_from_binary(Bucket, Key, Val),
            case put_merge(Coord, LWW, OldObj, RObj, VId, StartTime) of
                {oldobj, OldObj1} ->
                    {{false, OldObj1}, PutArgs};
                {newobj, NewObj} ->
                    VC = riak_object:vclock(NewObj),
                    AMObj = enforce_allow_mult(NewObj, BProps),
                    case IndexBackend of
                        true ->
                            IndexSpecs =
                                riak_object:diff_index_specs(AMObj,
                                                             OldObj);
                        false ->
                            IndexSpecs = []
                    end,
                    case PruneTime of
                        undefined ->
                            ObjToStore = AMObj;
                        _ ->
                            ObjToStore =
                                riak_object:set_vclock(AMObj,
                                                       vclock:prune(VC,
                                                                    PruneTime,
                                                                    BProps))
                    end,
                    {{true, ObjToStore},
                     PutArgs#putargs{index_specs=IndexSpecs, is_index=IndexBackend}}
            end
    end.

perform_put({false, Obj},
            _BE, Idx, _Trees,
            #putargs{returnbody=true,
                     reqid=ReqID}) ->
    {dw, Idx, Obj, ReqID};
perform_put({false, _Obj},
            _BE, Idx, _Trees,
            #putargs{returnbody=false,
                     reqid=ReqId}) ->
    {dw, Idx, ReqId};
perform_put({true, Obj},
            BE, Idx, Trees,
            #putargs{returnbody=RB,
                     bkey={Bucket, Key},
                     reqid=ReqID,
                     index_specs=IndexSpecs}) ->
    ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
    Val = riak_object:to_binary(ObjFmt, Obj),
    case beput(BE, Bucket, Key, IndexSpecs, Val) of
        ok ->
            update_hashtree(Bucket, Key, Val, Trees),
            case RB of
                true ->
                    {dw, Idx, Obj, ReqID};
                false ->
                    {dw, Idx, ReqID}
            end;
        {error, _Reason, _UpdModState} ->
            {fail, Idx, ReqID}
    end.

%% @private
put_merge(false, true, _CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=true
    {newobj, UpdObj};
put_merge(false, false, CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=false
    ResObj = riak_object:syntactic_merge(CurObj, UpdObj),
    case ResObj =:= CurObj of
        true ->
            {oldobj, CurObj};
        false ->
            {newobj, ResObj}
    end;
put_merge(true, true, _CurObj, UpdObj, VId, StartTime) -> % coord=false, LWW=true
    {newobj, riak_object:increment_vclock(UpdObj, VId, StartTime)};
put_merge(true, false, CurObj, UpdObj, VId, StartTime) ->
    UpdObj1 = riak_object:increment_vclock(UpdObj, VId, StartTime),
    UpdVC = riak_object:vclock(UpdObj1),
    CurVC = riak_object:vclock(CurObj),

    %% Check the coord put will replace the existing object
    case vclock:get_counter(VId, UpdVC) > vclock:get_counter(VId, CurVC) andalso
        vclock:descends(CurVC, UpdVC) == false andalso
        vclock:descends(UpdVC, CurVC) == true of
        true ->
            {newobj, UpdObj1};
        false ->
            %% If not, make sure it does
            {newobj, riak_object:increment_vclock(
                       riak_object:merge(CurObj, UpdObj1), VId, StartTime)}
    end.

%% @private
%% enforce allow_mult bucket property so that no backend ever stores
%% an object with multiple contents if allow_mult=false for that bucket
enforce_allow_mult(Obj, BProps) ->
    case proplists:get_value(allow_mult, BProps) of
        true -> Obj;
        _ ->
            case riak_object:get_contents(Obj) of
                [_] -> Obj;
                Mult ->
                    {MD, V} = select_newest_content(Mult),
                    riak_object:set_contents(Obj, [{MD, V}])
            end
    end.

%% @private
%% choose the latest content to store for the allow_mult=false case
select_newest_content(Mult) ->
    hd(lists:sort(
         fun({MD0, _}, {MD1, _}) ->
                 riak_core_util:compare_dates(
                   dict:fetch(<<"X-Riak-Last-Modified">>, MD0),
                   dict:fetch(<<"X-Riak-Last-Modified">>, MD1))
         end,
         Mult)).


-spec update_vnode_stats(vnode_get | vnode_put, partition(), erlang:timestamp()) ->
                                ok.
update_vnode_stats(Op, Idx, StartTS) ->
    riak_kv_stat:update({Op, Idx, timer:now_diff( os:timestamp(), StartTS)}).

update_index_write_stats(false, _IndexSpecs) ->
    ok;
update_index_write_stats(true, IndexSpecs) ->
    {Added, Removed} = count_index_specs(IndexSpecs),
    riak_kv_stat:update({vnode_index_write, Added, Removed}).

%% @private
%% update_index_delete_stats(IndexSpecs) ->
%%     {_Added, Removed} = count_index_specs(IndexSpecs),
%%     riak_kv_stat:update({vnode_index_delete, Removed}).

%% @private
%% @doc Given a list of index specs, return the number to add and
%% remove.
count_index_specs(IndexSpecs) ->
    %% Count index specs...
    F = fun({add, _, _}, {AddAcc, RemoveAcc}) ->
                {AddAcc + 1, RemoveAcc};
           ({remove, _, _}, {AddAcc, RemoveAcc}) ->
                {AddAcc, RemoveAcc + 1}
        end,
    lists:foldl(F, {0, 0}, IndexSpecs).

%% Backend get
beget({BEMod, BECtx}, Bucket, Key) ->
    {_Usecs, Result} = timer:tc(BEMod, get, [BECtx, Bucket, Key]),
    %% TODO: Update be stats
    Result.

%% Backend put
beput({BEMod, BECtx}, Bucket, Key, _IndexSpecs, Value) ->
    {_Usecs, Result} = timer:tc(BEMod, put, [BECtx, Bucket, Key, Value]),
    %% TODO: Update be stats
    Result.

%% Backend delete
%% bedel({BEMod, BECtx}, Bucket, Key) ->
%%     {Usec, Result} = timer:tc(BEMod, del, [BECtx, Bucket, Key]),
%%     %% TODO: Update be stats
%%     Result.

%-spec update_hashtree(binary(), binary(), binary(), state()) -> ok.
update_hashtree(Bucket, Key, Val, Trees) ->
    case get_hashtree_token() of
        true ->
            riak_kv_index_hashtree:async_insert_object({Bucket, Key}, Val, Trees),
            ok;
        false ->
            riak_kv_index_hashtree:insert_object({Bucket, Key}, Val, Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

%% TOOD: This needs a new mechanism.  Perhaps checkout on the put?
get_hashtree_token() ->
    Tokens = get(hashtree_tokens),
    case Tokens of
        undefined ->
            put(hashtree_tokens, max_hashtree_tokens() - 1),
            true;
        N when N > 0 ->
            put(hashtree_tokens, Tokens - 1),
            true;
        _ ->
            false
    end.

-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    app_helper:get_env(riak_kv,
                       anti_entropy_max_async, 
                       ?DEFAULT_HASHTREE_TOKENS).


object_from_binary({B,K}, ValBin) ->
    object_from_binary(B, K, ValBin).
object_from_binary(B, K, ValBin) ->
    case riak_object:from_binary(B, K, ValBin) of
        {error, R} -> throw(R);
        Obj -> Obj
    end.

