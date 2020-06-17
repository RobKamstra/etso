defmodule Etso.Adapter.TableSupervisor do
  use Supervisor
  @spec start_link(Etso.repo()) :: Supervisor.on_start()

  @doc """
  Starts the Supervisor for the given `repo`.
  """
  def start_link(config) do
    Supervisor.start_link(__MODULE__, config)
  end

  @impl Supervisor
  def init({repo, schema, _} = config) do
    application_config = Application.get_env(:summa_core, repo)

    children =
      if application_config do
        janitors = Keyword.get(application_config, :janitors, %{})

        janitor_config =
          janitors
          |> Map.get(schema, default_janitor_configuration())
          |> Map.put(:schema, schema)
          |> Map.put(:repo, repo)
          |> Map.put(:cache_entry_repo, Module.concat(repo, Cache))

        [
          {Etso.Adapter.TableServer, config},
          {Etso.Adapter.Janitor, janitor_config}
        ]
      else
        [
          {Etso.Adapter.TableServer, config}
        ]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp default_janitor_configuration() do
    %{
      cleaning_interval: 1_000,
      strategy: Etso.Cache.CleaningStrategy.LeastRecentlyWritten,
      options: [
        max_cache_entries: 100
      ]
    }
  end
end
