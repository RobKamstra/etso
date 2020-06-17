defmodule Etso.Cache.CleaningStrategy do
  @callback init(config :: keyword()) :: {:ok, term()} | {:error, term()}
  @callback cleanup_required?(strategy_state :: %{}, repo :: atom(), schema :: atom()) ::
              boolean()
  @callback cleanup(
              strategy_state :: %{},
              repo :: atom(),
              schema :: atom(),
              cache_entry_repo :: atom()
            ) :: %{}
  @callback cache_entry_added(strategy_state :: %{}, measurements :: %{}, metadata :: %{}) :: %{}
  @callback cache_entry_updated(strategy_state :: %{}, measurements :: %{}, metadata :: %{}) ::
              %{}
  @callback cache_entry_removed(strategy_state :: %{}, measurements :: %{}, metadata :: %{}) ::
              %{}
end
