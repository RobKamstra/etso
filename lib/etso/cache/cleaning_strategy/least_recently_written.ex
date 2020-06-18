defmodule Etso.Cache.CleaningStrategy.LeastRecentlyWritten do
  @behaviour Etso.Cache.CleaningStrategy

  alias Etso.Adapter.TableRegistry
  alias Etso.Cache.CachedQuery

  @impl Etso.Cache.CleaningStrategy
  def init(options) do
    {:ok,
     %{
       max_cache_entries: Keyword.get(options, :max_cache_entries, 1000),
       free_percentage: Keyword.get(options, :free_percentage, 0.2),
       queries: %{}
     }}
  end

  @impl Etso.Cache.CleaningStrategy
  def cleanup_required?(strategy_state, repo, schema) do
    {:ok, ref} = TableRegistry.get_table(repo, schema)
    ets_size = :ets.info(ref, :size)

    if ets_size > strategy_state.max_cache_entries do
      true
    else
      false
    end
  end

  @impl Etso.Cache.CleaningStrategy
  def cleanup(strategy_state, repo, _schema, cache_entry_repo) do
    {:ok, cached_query_table} = TableRegistry.get_table(cache_entry_repo, CachedQuery)
    free_percentage = strategy_state.free_percentage

    new_queries =
      Map.new(
        strategy_state.queries,
        &cleanup_query(cached_query_table, repo, free_percentage, &1)
      )

    Map.put(strategy_state, :queries, new_queries)
  end

  defp cleanup_query(
         _cached_query_table,
         _schema_repo,
         _free_percentage,
         {_, %{cache_entry_count: 0}} = query_stats
       ) do
    query_stats
  end

  defp cleanup_query(
         cached_query_table,
         schema_repo,
         free_percentage,
         {query_id, %{results_count: results_count, cache_entry_count: cache_entry_count}}
       ) do
    average_results_per_entry = results_count / cache_entry_count

    amount_to_be_freed_query = :math.ceil(results_count * free_percentage)

    amount_of_cached_queries_to_release =
      trunc(amount_to_be_freed_query / average_results_per_entry)

    ets_query =
      {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6", :"$7"}, [{:==, :"$1", query_id}],
       [{{:"$1", :"$2", :"$3", :"$4"}}]}

    # this needs fixing later as removed_entries will start going out of whack as multiple queries can have the same results
    {removed_cache_queries, removed_entries} =
      cached_query_table
      |> :ets.table(traverse: {:select, [ets_query]})
      |> :qlc.sort(
        order: fn {_, _, _, t1}, {_, _, _, t2} ->
          t1 < t2
        end
      )
      |> :qlc.cursor()
      |> erase_cursor(
        cached_query_table,
        schema_repo,
        amount_of_cached_queries_to_release,
        100,
        {0, 0}
      )

    {query_id,
     %{
       results_count: results_count - removed_entries,
       cache_entry_count: cache_entry_count - removed_cache_queries
     }}
  end

  @doc """
  This is called whenever a new cache entry is created
  """
  @impl Etso.Cache.CleaningStrategy
  def cache_entry_added(strategy_state, measurements, metadata) do
    query_id = metadata.query_id
    update_in(strategy_state, [:queries, query_id], &add_query_stats(measurements, &1))
  end

  defp add_query_stats(measurements, nil) do
    %{
      cache_entry_count: 1,
      results_count: measurements.count
    }
  end

  defp add_query_stats(measurements, %{
         cache_entry_count: count,
         results_count: results_count
       }) do
    new_count = count + 1

    %{
      cache_entry_count: new_count,
      results_count: results_count + measurements.count
    }
  end

  @impl Etso.Cache.CleaningStrategy
  def cache_entry_updated(strategy_state, _measurements, _metadata) do
    strategy_state
  end

  @impl Etso.Cache.CleaningStrategy
  def cache_entry_removed(strategy_state, measurements, metadata) do
    update_in(strategy_state, [:queries, metadata.query_id], fn query_stats ->
      query_stats
      |> Map.update(:cache_entry_count, 0, &(&1 - 1))
      |> Map.update(:results_count, 0, &(&1 - measurements.count))
    end)
  end

  # Erases entries in an LRW ETS cursor.
  #
  # This will exhaust a QLC cursor by taking in a provided cursor and removing the first
  # N elements (where N is the number of entries we need to remove). Removals are done
  # in configurable batches according to the `:batch_size` option.
  #
  # This is a recursive function as we have to keep track of the number to remove,
  # as the removal is done by calling `erase_batch/3`. At the end of the recursion,
  # we make sure to delete the trailing QLC cursor to avoid it lying around still.
  defp erase_cursor(
         cursor,
         table,
         schema_repo,
         remainder,
         batch_size,
         {removed_cached_queries, removed_entries}
       )
       when remainder > batch_size do
    {removed_cached_queries, removed_entries} =
      erase_batch(
        cursor,
        table,
        schema_repo,
        batch_size,
        {removed_cached_queries, removed_entries}
      )

    erase_cursor(
      cursor,
      table,
      schema_repo,
      remainder - batch_size,
      batch_size,
      {removed_cached_queries, removed_entries}
    )
  end

  defp erase_cursor(
         cursor,
         table,
         schema_repo,
         remainder,
         _batch_size,
         {removed_cached_queries, removed_entries}
       ) do
    {removed_cached_queries, removed_entries} =
      erase_batch(
        cursor,
        table,
        schema_repo,
        remainder,
        {removed_cached_queries, removed_entries}
      )

    :qlc.delete_cursor(cursor)
    {removed_cached_queries, removed_entries}
  end

  # Erases a batch of entries from a QLC cursor.
  #
  # This is not the most performant way to do this (as far as I know), as
  # we need to pull a batch of entries from the table and then just pass
  # them back through to ETS to erase them by key. This is nowhere near as
  # performant as `:ets.select_delete/2` but appears to be required because
  # of the need to sort the QLC cursor by the touch time.
  defp erase_batch(
         cursor,
         table,
         schema_repo,
         batch_size,
         {removed_cached_queries, removed_entries}
       ) do
    {adapter, adapter_meta} = Ecto.Repo.Registry.lookup(schema_repo)

    cursor
    |> :qlc.next_answers(batch_size)
    |> Enum.reduce({removed_cached_queries, removed_entries}, fn {query_id, params, count, _},
                                                                 {removed_cached_queries,
                                                                  removed_entries} ->
      :ets.delete(table, {query_id, params})
      query_cache = get_query_cache(adapter_meta, query_id)

      adapter.execute(
        adapter_meta,
        query_cache.query_meta,
        {:nocache,
         %{
           query: query_cache.query,
           strategy: :cache_only,
           type: :delete_all
         }},
        params,
        []
      )

      {removed_cached_queries + 1, removed_entries + count}
    end)
  end

  defp get_query_cache(adapter_meta, query_id) do
    [query_cache] =
      :ets.select(adapter_meta.cache, [
        {{:"$1", :"$2", :"$3", {:"$4", :"$5"}}, [{:==, :"$4", query_id}], [:"$5"]}
      ])

    query_cache
  end
end
