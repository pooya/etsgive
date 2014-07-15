-module(ets_keeper).

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, give_tab2me/1, delete_tab/1]).

-define(SECOND, 1000).
-define(MINUTE, (60 * ?SECOND)).
-define(HOUR, (60 * ?MINUTE)).
-define(DAY, (24 * ?HOUR)).

-type gs_init()      :: {ok, state()}.
-type gs_reply(T)    :: {reply, (T), state()}.
-type gs_noreply()   :: {noreply, state()}.

-define(ETS_TABLE_VALID, (10 * ?DAY)).
-type state() :: gb_trees:tree(atom(), {pid(), ets:tab(), erlang:timestamp()}).

-spec start_link() -> no_return().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init([]) -> gs_init().
init([]) ->
    process_flag(trap_exit, true),
    {ok, gb_trees:empty()}.

-spec give_tab2me(atom()) -> boolean().
give_tab2me(TabName) ->
    gen_server:call(?MODULE, {give_tab2pid, TabName, self()}).

-spec delete_tab(atom()) -> ok.
delete_tab(TabName) ->
    gen_server:call(?MODULE, {del_tab, TabName, self()}).

-spec handle_call({give_tab2pid | del_tab, atom(), pid()}, pid(), state()) -> gs_reply(ok).
handle_call({give_tab2pid, TabName, Pid}, _From, Tree) ->
    {Created, NewTree} = do_give_tab2pid(TabName, Pid, Tree),
    {reply, Created, NewTree};
handle_call({del_tab, TabName, Pid}, _From, Tree) ->
    {reply, ok, do_del_tab(TabName, Pid, Tree)}.

-spec do_del_tab(atom(), pid(), state()) -> state().
do_del_tab(TabName, Pid, Tree) -> 
    % make sure table is present and belongs to the Pid
    {Pid, _, _} = gb_trees:get(TabName, Tree),
    ets:delete(TabName),
    gb_trees:delete(TabName, Tree).


-spec do_give_tab2pid(atom(), pid(), state()) -> {boolean(), state()}.
do_give_tab2pid(TabName, Pid, Tree) ->
    case gb_trees:lookup(TabName, Tree) of
        none ->
            {true, create_new_table(TabName, Pid, Tree)};
        {value, {none, TabId, CreationTime}} ->
            Since = timer:now_diff(now(), CreationTime),
            case Since < ?ETS_TABLE_VALID of
                true ->
                    ets:delete(TabId),
                    {true, create_new_table(TabName, Pid, Tree)};
                false ->
                    ets:give_away(TabId, Pid, CreationTime),
                    {false, gb_trees:update(TabName, {Pid, TabId, CreationTime}, Tree)}
            end
    end.

-spec create_new_table(atom(), pid(), state()) -> state().
create_new_table(TabName, Pid, Tree) ->
    TabId = ets:new(TabName, [named_table, set, private]),
    ets:setopts(TabId, {heir, self(), {}}),
    gb_trees:insert(TabName, {Pid, TabId, now()}, Tree).

-spec handle_info({'ETS-TRANSFER', ets:tab(), pid(), {}}, state()) -> gs_noreply().
    handle_info({'ETS-TRANSFER', TabId, Pid, {}}, Tree) ->
    NewTree = case ets:info(TabId, name) of
        undefined ->
            Tree;
        TabName ->
            % make sure the Pid was the owner of the ets table and then update
            % this information in state.
            {Pid, TabId, CreationTime} = gb_trees:get(TabName, Tree),
            gb_trees:update(TabName, {none, TabId, CreationTime}, Tree)
    end,
    {noreply, NewTree}.

-spec handle_cast(term(), state()) -> gs_noreply().
handle_cast(_, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
