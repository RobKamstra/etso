defmodule Etso.Adapter.Supervisor do
  @moduledoc """
  Repo-level Supervisor which supports the ETS Adapter. Within the Supervision Tree, a Dynamic
  Supervisor is used to hold the Table Servers, and a Registry is used to keep track of both the
  Table Servers, and their ETS Tables.
  """

  use Supervisor
  @spec start_link(Etso.repo()) :: Supervisor.on_start()

  @doc """
  Starts the Supervisor for the given `repo`.
  """
  def start_link(config) do
    Supervisor.start_link(__MODULE__, config)
  end

  @impl Supervisor
  def init(config) do
    {:ok, repo} = Keyword.fetch(config, :repo)
    cache_for = Keyword.get(config, :cache_for, nil)

    children =
      if cache_for do
        cache = Module.concat([repo, Cache])

        [
          {cache, [name: cache]},
          {Etso.Adapter.CacheSupervisor, repo},
          {Etso.Adapter.TableRegistry, repo}
        ]
      else
        [
          {Etso.Adapter.CacheSupervisor, repo},
          {Etso.Adapter.TableRegistry, repo}
        ]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
