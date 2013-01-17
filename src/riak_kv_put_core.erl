%% -------------------------------------------------------------------
%%
%% riak_kv_put_core: Riak put logic
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_kv_put_core).
-export([init/10, add_result/2, enough/1, response/1, 
         final/1, result_shortcode/1, result_idx/1]).
-export_type([putcore/0, result/0, reply/0]).

-type vput_result() :: any().

-type result() :: w |
                  {dw, undefined} |
                  {dw, riak_object:riak_object()} |
                  {error, any()}.

-type reply() :: ok | 
                 {ok, riak_object:riak_object()} |
                 {error, notfound} |
                 {error, any()}.
-type idxresult() :: {non_neg_integer(), result()}.
-record(putcore, {n :: pos_integer(),
                  w :: non_neg_integer(),
                  dw :: non_neg_integer(),
                  pw :: non_neg_integer(),
                  w_fail_threshold :: pos_integer(),
                  pw_fail_threshold :: pos_integer(),
                  dw_fail_threshold :: pos_integer(),
                  returnbody :: boolean(),
                  allowmult :: boolean(),
                  results = [] :: [idxresult()],
                  final_obj :: undefined | riak_object:riak_object(),
                  num_w = 0 :: non_neg_integer(),
                  num_dw = 0 :: non_neg_integer(),
                  num_pw = 0 :: non_neg_integer(),
                  num_fail = 0 :: non_neg_integer(),
                  idx_type :: orddict:orddict() %% mapping of idx -> primary | fallback
                 }).
-opaque putcore() :: #putcore{}.

%% ====================================================================
%% Public API
%% ====================================================================

%% Initialize a put and return an opaque put core context
-spec init(pos_integer(), non_neg_integer(), non_neg_integer(), 
           non_neg_integer(), pos_integer(), pos_integer(),
           pos_integer(), boolean(), boolean(),
           orddict:orddict()) -> putcore().
init(N, W, PW, DW, WFailThreshold, PWFailThreshold,
     DWFailThreshold, AllowMult, ReturnBody, IdxType) ->
    #putcore{n = N, w = W, pw = PW, dw = DW,
             w_fail_threshold = WFailThreshold,
             pw_fail_threshold = PWFailThreshold,
             dw_fail_threshold = DWFailThreshold,
             allowmult = AllowMult,
             returnbody = ReturnBody,
             idx_type = IdxType}.

%% @private If the Idx is not in the IdxType
%% the world should end
is_primary_response(Idx, IdxType) ->
    {ok, Status} = orddict:find(Idx, IdxType),
    Status == primary.

num_pw(PutCore = #putcore{num_pw=NumPW, idx_type=IdxType}, Idx) ->
    case is_primary_response(Idx, IdxType) of
        true ->
            PutCore#putcore{num_pw=NumPW+1};
        false ->
            PutCore
    end.

%% Add a result from the vnode
-spec add_result(vput_result(), putcore()) -> putcore().
add_result({w, Idx, _ReqId}, PutCore = #putcore{results = Results,
                                                num_w = NumW}) ->
    num_pw(PutCore#putcore{results = [{Idx, w} | Results],
                    num_w = NumW + 1}, Idx);
add_result({dw, Idx, _ReqId}, PutCore = #putcore{results = Results,
                                                 num_dw = NumDW}) ->
    PutCore#putcore{results = [{Idx, {dw, undefined}} | Results],
                    num_dw = NumDW + 1};
add_result({dw, Idx, ResObj, _ReqId}, PutCore = #putcore{results = Results,
                                                         num_dw = NumDW}) ->
    PutCore#putcore{results = [{Idx, {dw, ResObj}} | Results],
                    num_dw = NumDW + 1};
add_result({fail, Idx, _ReqId}, PutCore = #putcore{results = Results,
                                                   num_fail = NumFail}) ->
    PutCore#putcore{results = [{Idx, {error, undefined}} | Results],
                    num_fail = NumFail + 1};
add_result(_Other, PutCore = #putcore{num_fail = NumFail}) ->
    %% Treat unrecognized messages as failures - no index to store them against
    PutCore#putcore{num_fail = NumFail + 1}.

%% Check if enough results have been added to respond
-spec enough(putcore()) -> boolean().
%% The perfect world, all the quorum restrictions have been met.
enough(#putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, pw = PW, num_pw = NumPW}) when
      NumW >= W, NumDW >= DW, NumPW >= PW ->
    true;
%% Enough failures that we can't meet the DW restriction
enough(#putcore{ w = W, num_w = NumW, num_fail = NumFail, dw_fail_threshold = DWFailThreshold}) when
      NumW >= W, NumFail >= DWFailThreshold ->
    true;
%% Enough failures that we can't meet the PW restriction
enough(#putcore{ w = W, num_w = NumW, num_fail = NumFail, pw_fail_threshold = PWFailThreshold}) when
      NumW >= W, NumFail >= PWFailThreshold ->
    true;
enough(#putcore{ w = W, num_w = NumW, num_fail = NumFail, w_fail_threshold = WFailThreshold}) when
      NumW < W, NumFail >= WFailThreshold ->
    true;
enough(_) ->
    false.

%% Get success/fail response once enough results received
-spec response(putcore()) -> {reply(), putcore()}.
%% Perfect world - all quora met
response(PutCore = #putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, pw = PW, num_pw = NumPW}) when
      NumW >= W, NumDW >= DW, NumPW >= PW ->
    maybe_return_body(PutCore);
%% Everything is ok, except we didn't meet PW
response(PutCore = #putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, pw = PW, num_pw = NumPW}) when
      NumW >= W, NumDW >= DW, NumPW < PW ->
    {{error, pw_val_unsatisfied, PW, NumPW}, PutCore};
%% Didn't make DW
response(PutCore = #putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW}) when
      NumW >= W, NumDW < DW ->
    {{error, dw_val_unsatisfied, DW, NumDW}, PutCore};
%% Didn't even make quorom
response(PutCore = #putcore{w = W, num_w = NumW}) ->
    {{error, w_val_unsatisfied, W, NumW}, PutCore}.

%% Get final value - if returnbody did not need the result it allows delaying
%% running reconcile until after the client reply is sent.
-spec final(putcore()) -> {riak_object:riak_object()|undefined, putcore()}.
final(PutCore = #putcore{final_obj = FinalObj, 
                         results = Results, allowmult = AllowMult}) ->
    case FinalObj of
        undefined ->
            RObjs = [RObj || {_Idx, {dw, RObj}} <- Results, RObj /= undefined],
            ReplyObj = case RObjs of
                           [] ->
                               undefined;
                           _ ->
                               riak_object:reconcile(RObjs, AllowMult)
                       end,
            {ReplyObj, PutCore#putcore{final_obj = ReplyObj}};
        _ ->
            {FinalObj, PutCore}
    end.

result_shortcode({w, _, _})     -> 1;
result_shortcode({dw, _, _})    -> 2;
result_shortcode({dw, _, _, _}) -> 2;
result_shortcode({fail, _, _})  -> -1;
result_shortcode(_)             -> -2.

result_idx({_, Idx, _})    -> Idx;
result_idx({_, Idx, _, _}) -> Idx;
result_idx(_)              -> -1.

%% ====================================================================
%% Internal functions
%% ====================================================================
maybe_return_body(PutCore = #putcore{returnbody = false}) ->
    {ok, PutCore};
maybe_return_body(PutCore = #putcore{returnbody = true}) ->
    {ReplyObj, UpdPutCore} = final(PutCore),
    {{ok, ReplyObj}, UpdPutCore}.

