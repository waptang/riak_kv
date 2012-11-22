%% -------------------------------------------------------------------
%%
%% riak_object_eqc: serialization/deserialization of riak_object for disk/wire
%%                  and converting between versions
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
-module(riak_object_eqc).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-compile(export_all).

roundtrip_eqc_test_() ->
    Res = eqc:quickcheck(numtests(1000, ?QC_OUT(prop_serialize_deserialize()))),
    ?_assertEqual(true, Res).

prop_serialize_deserialize() ->
    ?FORALL(Obj, 
            fsm_eqc_util:riak_object(),
            riak_object:equal(
              Obj, 
              riak_object:binary_to_robj(
                riak_object:bucket(Obj),
                riak_object:key(Obj),
                riak_object:robj_to_binary(Obj)))).


-endif. %% EQC
