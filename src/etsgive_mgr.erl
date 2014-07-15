-module(etsgive_mgr).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {table_id}).

gift() ->
    gen_server:cast(?MODULE, {gift, {count, 0}}).

start_link() ->
        gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    gift(),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
        Reply = ok,
        {reply, Reply, State}.

handle_cast({gift, Data}, State) ->
        TableId = ets:new(?MODULE, [private]),
        ets:insert(TableId, Data),
        ets:setopts(TableId, {heir, self(), Data}),
        giveaway_table(TableId, Data),
        {noreply, State#state{table_id=TableId}};
handle_cast(_Msg, State) ->
        {noreply, State}.

giveaway_table(TableId, Data) ->
    try
        SRV = wait_for_srv(),
        link(SRV),
        ets:give_away(TableId, SRV, Data)
    catch K:V ->
        io:format("Problem giving ets table away ~p ~p", [K, V]),
        giveaway_table(TableId, Data)
    end.

handle_info({'EXIT',Pid,killed}, State) ->
    TableId = State#state.table_id,
    io:format("SRV(~p) !! is now dead, farewell TableId: ~p~n", [Pid, TableId]),
    {noreply, State};
handle_info({'ETS-TRANSFER', TableId, Pid, Data}, State) ->
    io:format("Warning TableId: ~p OwnerPid: ~p is dying~n"
              "SRV(~p) => MGR(~p) handing TableId: ~p~n", [TableId, Pid, Pid, self(), TableId]),
    giveaway_table(TableId, Data),
    {noreply, State#state{table_id=TableId}}.

wait_for_srv() -> 
    case whereis(etsgive_srv) of
        undefined -> 
            timer:sleep(1),
            wait_for_srv();
        Pid -> Pid
    end.

terminate(_Reason, _State) ->
        ok.
code_change(_OldVsn, State, _Extra) ->
        {ok, State}.
