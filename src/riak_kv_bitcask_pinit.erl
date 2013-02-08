-module(riak_kv_bitcask_pinit).
-export([run/0]).

run() ->
    MaxConcurrent = app_helper:get_env(riak_kv, bitcask_pinit, 4),
    lager:debug("Pre-loading bitcasks in parallel (~p)", [MaxConcurrent]),
    pload_bitcask(MaxConcurrent).

pload_bitcask(MaxConcurrent) ->
    lager:debug("Will pre-load Bitcask in parallel (~p)", [MaxConcurrent]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:indices(Ring, node()),
    Config = app_helper:get_env(riak_kv),
    DataRoot =  app_helper:get_prop_or_env(data_root, Config, bitcask),
    ToDir = fun(I) ->
                    filename:join([DataRoot, integer_to_list(I)])
            end,
    Dirs = lists:filter(fun filelib:is_dir/1, [ ToDir(I) || I <- Indices]),
    Work = [ fun() ->
                     lager:debug("Pre-opening bitcask on ~s", [Dir]),
                     BRef = bitcask:open(Dir, [{expiry_secs, -1},
                                               {open_timeout, 1000},
                                               {read_write, false}]),
                     % Keep keydir reference stored in process by bitcask:open
                     % around until KV vnodes have initialized
                     BData = get(BRef),
                     spawn(fun()->
                                   riak_core:wait_for_service(riak_kv),
                                   lager:debug("KV service is up, so I'm releasing the keydir ~s ~p", [Dir, BData])
                           end),
                     lager:debug("*FINISHED* Pre-opening bitcask on ~s", [Dir])
             end 
           || Dir <- Dirs],
    do_concurrent_work(Work, MaxConcurrent),
    lager:debug("*FINISHED* pre-loading bitcasks in parallel"),
    ok.

%% Execute functions in parallel with up to MaxConcurrent processes at a time
do_concurrent_work(WorkList, MaxConcurrent) ->
    lists:foldl(fun(X, Acc) ->
                        do_concurrent_work(X, Acc, MaxConcurrent) 
                end, [], WorkList ++ [end_of_work]).

do_concurrent_work(end_of_work, [], _) ->
    ok;
% Spin up another worker if less than max concurrency
do_concurrent_work(WorkFun, Workers, Max)
  when is_function(WorkFun) andalso length(Workers) < Max ->
    Pid = spawn(WorkFun),
    Ref = erlang:monitor(process, Pid),
    [Ref | Workers];
% Stop and wait for a worker to finish if max or end of work marker reached
do_concurrent_work(WorkFun, Working, Max)
  when WorkFun =:= end_of_work orelse length(Working) == Max ->
    receive
        {'DOWN', Ref, process, _Pid, _Reason} ->
            do_concurrent_work(WorkFun, lists:delete(Ref, Working), Max)
    end.


