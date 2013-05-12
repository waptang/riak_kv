-module(riak_kv_eleveldb_backend3).

%% Process to manage all running eleveldb backends
-export([api_version/0, capabilities/1, capabilities/2]).
-export([start_link/1, open/1, stop/1, drop/1, get/3, put/4, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {open = [],
                config}).
%% -type state() :: #state{}.
%% -type config() :: [{atom(), term()}].

-record(ldb_ctx, {ref,
                  data_root :: string(),
                  open_opts = [],
                  read_opts = [],
                  write_opts = [],
                  fold_opts = [{fill_cache, false}]}).

-define(API_VERSION, 3).
-define(CAPABILITIES, []).

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.


start_link([]) ->
    Config = application:get_all_env(eleveldb),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

open(Partition) ->
    gen_server:call(?MODULE, {open, Partition}, infinity).

stop(Ctx) ->
    gen_server:call(?MODULE, {stop, Ctx}, infinity).

drop(Ctx) ->
    gen_server:call(?MODULE, {drop, Ctx}, infinity).

%% Can be run from any process
get(#ldb_ctx{ref = Ref, read_opts = ReadOpts}, Bucket, Key) ->
    StorageKey = to_object_key(Bucket, Key),
    case eleveldb:get(Ref, StorageKey, ReadOpts) of
        {ok, Value} ->
            {ok, Value};
        not_found  ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.

put(#ldb_ctx{ref = Ref, write_opts = WriteOpts},
    Bucket, PrimaryKey, Val) ->

    %% Create the KV update...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{put, StorageKey, Val}],

    %% Convert IndexSpecs to index updates...
    %% F = fun({add, Field, Value}) ->
    %%             {put, to_index_key(Bucket, PrimaryKey, Field, Value), <<>>};
    %%        ({remove, Field, Value}) ->
    %%             {delete, to_index_key(Bucket, PrimaryKey, Field, Value)}
    %%     end,
    %% Updates2 = [F(X) || X <- IndexSpecs],

    %% Perform the write...
    %% case eleveldb:write(Ref, Updates1 ++ Updates2, WriteOpts) of
    case eleveldb:write(Ref, Updates1, WriteOpts) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get the status information for this eleveldb backend
status(Ctx) ->
    {ok, Stats} = eleveldb:status(Ctx#ldb_ctx.ref, <<"leveldb.stats">>),
    {ok, ReadBlockError} = eleveldb:status(Ctx#ldb_ctx.ref, <<"leveldb.ReadBlockError">>),
    [{stats, Stats}, {read_block_error, ReadBlockError}].

init(Config) ->
    {ok, #state{config = Config}}.

make_partition_dir(Partition, #state{config = Config}) ->
    filename:join(app_helper:get_prop_or_env(data_root, Config, eleveldb),
                  integer_to_list(Partition)).

handle_call({open, Partition}, _From, State = #state{config = Config,
                                                     open = Open}) ->
    DataDir = make_partition_dir(Partition, State),
    Ctx0 = init_ctx(DataDir, Config),
    case open_db(DataDir, Ctx0) of
        {error, _Reason}=ER ->
            {reply, ER, State};
        {ok, Ctx} ->
            State2 = State#state{open = [Ctx | Open]},
            {reply, {ok, Ctx}, State2}
    end;
handle_call({stop, #ldb_ctx{ref = Ref}}, _From, State = #state{open = Open}) ->
    eleveldb:close(Ref),
    {reply, ok, State#state{ open = lists:keydelete(Ref, #ldb_ctx.ref, Open) } };
handle_call({drop, #ldb_ctx{ref = Ref, data_root = DataRoot}},
            _From, State = #state{open = Open}) ->
    eleveldb:close(Ref),
    eleveldb:destroy(DataRoot, []),
    {reply, ok, State#state{ open = lists:keydelete(Ref, #ldb_ctx.ref, Open) } }.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
init_ctx(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "dummy")),

    %% Merge the proplist passed in from Config with any values specified by the
    %% eleveldb app level; precedence is given to the Config.
    MergedConfig = orddict:merge(fun(_K, VLocal, _VGlobal) -> VLocal end,
                                 orddict:from_list(Config), % Local
                                 orddict:from_list(application:get_all_env(eleveldb))), % Global

    %% Use a variable write buffer size in order to reduce the number
    %% of vnodes that try to kick off compaction at the same time
    %% under heavy uniform load...
    WriteBufferMin = config_value(write_buffer_size_min, MergedConfig, 30 * 1024 * 1024),
    WriteBufferMax = config_value(write_buffer_size_max, MergedConfig, 60 * 1024 * 1024),
    WriteBufferSize = WriteBufferMin + random:uniform(1 + WriteBufferMax - WriteBufferMin),

    %% Update the write buffer size in the merged config and make sure create_if_missing is set
    %% to true
    FinalConfig = orddict:store(write_buffer_size, WriteBufferSize,
                                orddict:store(create_if_missing, true, MergedConfig)),

    %% Parse out the open/read/write options
    {OpenOpts, _BadOpenOpts} = eleveldb:validate_options(open, FinalConfig),
    {ReadOpts, _BadReadOpts} = eleveldb:validate_options(read, FinalConfig),
    {WriteOpts, _BadWriteOpts} = eleveldb:validate_options(write, FinalConfig),

    %% Use read options for folding, but FORCE fill_cache to false
    FoldOpts = lists:keystore(fill_cache, 1, ReadOpts, {fill_cache, false}),

    %% Warn if block_size is set
    SSTBS = proplists:get_value(sst_block_size, OpenOpts, false),
    BS = proplists:get_value(block_size, OpenOpts, false),
    case BS /= false andalso SSTBS == false of
        true ->
            lager:warning("eleveldb block_size has been renamed sst_block_size "
                          "and the current setting of ~p is being ignored.  "
                          "Changing sst_block_size is strongly cautioned "
                          "against unless you know what you are doing.  Remove "
                          "block_size from app.config to get rid of this "
                          "message.\n", [BS]);
        _ ->
            ok
    end,

    %% Generate a debug message with the options we'll use for each operation
    lager:debug("Datadir ~s options for LevelDB: ~p\n",
                [DataRoot, [{open, OpenOpts}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #ldb_ctx{data_root = DataRoot,
             open_opts = OpenOpts,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts}.

%% @private
open_db(Dir, Ctx) ->
    RetriesLeft = app_helper:get_env(riak_kv, eleveldb_open_retries, 30),
    open_db(Dir, Ctx, max(1, RetriesLeft), undefined).

open_db(_Dir, _Ctx0, 0, LastError) ->
    {error, LastError};
open_db(Dir, Ctx0, RetriesLeft, _) ->
    case eleveldb:open(Dir, Ctx0#ldb_ctx.open_opts) of
        {ok, Ref} ->
            {ok, Ctx0#ldb_ctx { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed vnode takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = app_helper:get_env(riak_kv, eleveldb_open_retry_delay, 2000),
                    lager:debug("Leveldb backend retrying ~p in ~p ms after error ~s\n",
                                [Dir, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(Dir, Ctx0, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
config_value(Key, Config, Default) ->
    case orddict:find(Key, Config) of
        error ->
            Default;
        {ok, Value} ->
            Value
    end.
to_object_key(Bucket, Key) ->
    sext:encode({o, Bucket, Key}).

%% from_object_key(LKey) ->
%%     case sext:decode(LKey) of
%%         {o, Bucket, Key} ->
%%             {Bucket, Key};
%%         _ ->
%%             undefined
%%     end.

%% to_index_key(Bucket, Key, Field, Term) ->
%%     sext:encode({i, Bucket, Field, Term, Key}).

%% from_index_key(LKey) ->
%%     case sext:decode(LKey) of
%%         {i, Bucket, Field, Term, Key} ->
%%             {Bucket, Key, Field, Term};
%%         _ ->
%%             undefined
%%     end.
