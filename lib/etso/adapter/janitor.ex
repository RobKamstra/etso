defmodule Etso.Adapter.Janitor do
  use GenServer

  require Logger

  def start_link(janitor_configuration) do
    GenServer.start_link(__MODULE__, janitor_configuration)
  end

  def stop(schema) do
    GenServer.stop(schema)
  end

  @impl GenServer
  def init(configuration) do
    strategy = Map.get(configuration, :strategy)
    strategy_options = Map.get(configuration, :options)
    cleaning_interval = Map.get(configuration, :cleaning_interval)

    uuid = UUID.uuid4()

    events = [
      [:etso, :internal, :write_cache_entry, configuration.schema],
      [:etso, :internal, :update_cache_entry, configuration.schema],
      [:etso, :internal, :remove_cache_entry, configuration.schema]
    ]

    :ok = :telemetry.attach_many(uuid, events, &handle_event/4, self())

    case strategy.init(strategy_options) do
      {:ok, strategy_state} ->
        schedule_next_cleanup(cleaning_interval)
        state = Map.put(configuration, :strategy_state, strategy_state)
        {:ok, state}

      error ->
        error
    end
  end

  def handle_event([:etso, :internal, event, _], measurements, metadata, janitor_pid) do
    GenServer.cast(janitor_pid, {event, measurements, metadata})
  end

  @impl GenServer
  def terminate(reason, _state) do
    Logger.info(reason)
  end

  @impl GenServer
  def handle_cast({:write_cache_entry, measurements, metadata}, state) do
    strategy = Map.get(state, :strategy)
    strategy_state = Map.get(state, :strategy_state)

    new_strategy_state = strategy.cache_entry_added(strategy_state, measurements, metadata)
    new_state = Map.put(state, :strategy_state, new_strategy_state)

    {:noreply, new_state}
  end

  def handle_cast({:update_cache_entry, measurements, metadata}, state) do
    strategy = Map.get(state, :strategy)
    strategy_state = Map.get(state, :strategy_state)

    new_strategy_state = strategy.cache_entry_updated(strategy_state, measurements, metadata)
    new_state = Map.put(state, :strategy_state, new_strategy_state)

    {:noreply, new_state}
  end

  def handle_cast({:remove_cache_entry, measurements, metadata}, state) do
    strategy = Map.get(state, :strategy)
    strategy_state = Map.get(state, :strategy_state)

    new_strategy_state = strategy.cache_entry_removed(strategy_state, measurements, metadata)
    new_state = Map.put(state, :strategy_state, new_strategy_state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:cleanup, state) do
    strategy = Map.get(state, :strategy)
    strategy_state = Map.get(state, :strategy_state)
    repo = Map.get(state, :repo)
    schema = Map.get(state, :schema)
    cache_entry_repo = Map.get(state, :cache_entry_repo)

    strategy_state =
      if strategy.cleanup_required?(strategy_state, repo, schema) do
        strategy.cleanup(strategy_state, repo, schema, cache_entry_repo)
      else
        strategy_state
      end

    schedule_next_cleanup(state.cleaning_interval)

    new_state = Map.put(state, :strategy_state, strategy_state)
    {:noreply, new_state}
  end

  # # :ets.select(adapter_meta.cache, [{{:"$1", :"$2", :"$3", {:"$4", :"$5"}}, [{:==, :"$4", query_id}], [:"$1"]}])

  def get_memory_usage(schema) do
    {:ok, ets_table} = Etso.Adapter.TableRegistry.get_table(EtsoRepo, schema)
    :ets.info(ets_table, :memory)
  end

  defp schedule_next_cleanup(interval) do
    Process.send_after(self(), :cleanup, interval)
  end
end
