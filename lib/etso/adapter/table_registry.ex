defmodule Etso.Adapter.TableRegistry do
  @moduledoc """
  Provides convenience function to spin up a Registry, which is used to hold the Table Servers
  (registered by GenServer when starting up), alongside their ETS tables (registered when the
  Table Server starts).
  """

  @spec child_spec(Etso.repo()) :: Supervisor.child_spec()
  @spec get_table(Etso.repo(), Etso.schema()) :: {:ok, Etso.table()} | {:error, term()}
  @spec register_table(Etso.repo(), Etso.schema(), Etso.table()) :: :ok | {:error, term()}

  alias Etso.Adapter.TableSupervisor
  alias Etso.Adapter.CacheSupervisor

  @doc """
  Returns Child Specification for the Table Registry that will be associated with the `repo`.
  """
  def child_spec(repo) do
    Registry.child_spec(keys: :unique, name: build_name(repo))
  end

  def disable_table(repo, schema) do
    registry = build_name(repo)

    :persistent_term.put({repo, schema, :override_strategy}, :original_only)

    case Registry.lookup(registry, {schema, :supervisor}) do
      [{pid, _table_supervisor_reference}] ->
        CacheSupervisor.stop_child(repo, pid)

      [] ->
        {:error, :not_found}
    end
  end

  def enable_table(repo, schema) do
    :persistent_term.put({repo, schema, :override_strategy}, :cache_or_original)
  end

  @doc """
  Returns the ETS table associated with the given `repo` which is used to hold data for `schema`.
  """
  def get_table(repo, schema) do
    case lookup_table(repo, schema) do
      {:ok, table_reference} ->
        {:ok, table_reference}

      {:error, :not_found} ->
        start_table(repo, schema)
    end
  end

  @doc """
  Registers the ETS table associated with the given `repo` which is used to hold data for `schema`.
  """
  def register_table(repo, schema, table_reference) do
    with {:ok, _} <- Registry.register(build_name(repo), {schema, :ets_table}, table_reference) do
      :ok
    end
  end

  def unregister_table(repo, schema) do
    registry = build_name(repo)
    Registry.unregister(registry, {schema, :ets_table})
    Registry.lookup(registry, {schema, :ets_table})
  end

  defp lookup_table(repo, schema) do
    case Registry.lookup(build_name(repo), {schema, :ets_table}) do
      [{_, table_reference}] -> {:ok, table_reference}
      [] -> {:error, :not_found}
    end
  end

  defp start_table(repo, schema) do
    with {:ok, _} <- ensure_server_started(repo, schema) do
      lookup_table(repo, schema)
    end
  end

  defp ensure_server_started(repo, schema) do
    case start_server(repo, schema) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      _ -> :error
    end
  end

  defp start_server(repo, schema) do
    name = {:via, Registry, {build_name(repo), {schema, :supervisor}}}
    child_spec = {TableSupervisor, {repo, schema, name}}
    CacheSupervisor.start_child(repo, child_spec)
  end

  defp build_name(repo) do
    Module.concat([repo, Enum.at(Module.split(__MODULE__), -1)])
  end
end
