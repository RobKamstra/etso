defmodule Etso.Adapter.Behaviour.Queryable do
  @moduledoc false

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.MatchSpecification
  alias Etso.ETS.TableStructure
  import Ecto.Query

  def prepare(type, query) do
    id = System.unique_integer([:positive])
    schema = get_schema_from_query(query)

    query_cache = %{
      type: type,
      query: query,
      schema: schema
    }

    {:cache, {id, query_cache}}
  end

  def execute(adapter_meta, query_meta, qc, params, options) do
    {id, query_cache} = update_query_cache(adapter_meta, query_meta, qc, options)

    strategy =
      :persistent_term.get(
        {adapter_meta.repo, query_cache.schema, :override_strategy},
        query_cache.strategy
      )

    case strategy do
      :original_only ->
        query_original(adapter_meta.cache_for, query_meta, query_cache, params, options)

      :cache_only ->
        query_on_cache(adapter_meta.repo, query_cache, params)

      :cache_or_original ->
        with(
          {:supported_query, true} <- {:supported_query, is_supported_query?(query_cache.query)},
          {:cached, cached_query} when is_map(cached_query) <-
            {:cached, is_cached?(adapter_meta.entry_repo, id, params)}
        ) do
          {count, _} = result = query_on_cache(adapter_meta.repo, query_cache, params)
          #
          if cached_query.expected_count != count do
            delete_cache_entry(adapter_meta.entry_repo, cached_query)

            :telemetry.execute(
              [:etso, :internal, :remove_cache_entry, query_cache.schema],
              %{
                count: cached_query.expected_count
              },
              %{
                query_id: id,
                params: params
              }
            )

            execute(adapter_meta, query_meta, qc, params, options)
          else
            update_cache_entry(adapter_meta.entry_repo, cached_query, query_cache.schema)
            result
          end
        else
          {:supported_query, false} ->
            query_original(adapter_meta.cache_for, query_meta, query_cache, params, options)

          {:cached, false} ->
            resultset =
              query_original(adapter_meta.cache_for, query_meta, query_cache, params, options)

            if query_cache.type == :all do
              write_resultset_in_cache(id, params, resultset, adapter_meta, query_meta)
            end

            resultset
        end
    end
  end

  defp is_supported_query?(query) do
    no_joins = Enum.empty?(query.joins)
    no_orders = Enum.empty?(query.order_bys)
    no_groups = Enum.empty?(query.group_bys)
    no_havings = Enum.empty?(query.havings)
    no_joins and no_orders and no_groups and no_havings
  end

  defp delete_cache_entry(entry_repo, cache_entry) do
    entry_repo.delete(cache_entry)
  end

  defp update_cache_entry(entry_repo, cache_entry, schema) do
    access_time = System.os_time(:microsecond)
    time_between_previous_access = access_time - cache_entry.updated_at

    old_average_time_between_access =
      cache_entry.average_time_between_access * cache_entry.times_accessed

    new_times_accessed = cache_entry.times_accessed + 1

    new_average_time_between_access =
      trunc((old_average_time_between_access + time_between_previous_access) / new_times_accessed)

    :telemetry.execute(
      [:etso, :internal, :update_cache_entry, schema],
      %{
        time_between_previous_access: time_between_previous_access,
        times_accessed: new_times_accessed,
        average_time_between_access: new_average_time_between_access
      },
      %{
        query_id: cache_entry.query_id
      }
    )

    changeset =
      Etso.Cache.CachedQuery.changeset(cache_entry, %{
        times_accessed: new_times_accessed,
        updated_at: access_time,
        average_time_between_access: new_average_time_between_access
      })

    entry_repo.update(changeset, [])
  end

  defp write_resultset_in_cache(query_id, params, resultset, adapter_meta, query_meta) do
    {count, results} = resultset
    {:any, {:source, {_table, schema}, _prefix, _types}} = query_meta.select.from

    has_results? = count > 0
    complete_resultset? = query_meta.select.postprocess == {:source, :from}
    insert_into_cache? = has_results? and complete_resultset?

    entry_repo = adapter_meta.entry_repo

    if insert_into_cache? do
      field_names = schema.__schema__(:fields)
      entries = Enum.map(results, &Enum.zip(field_names, &1))
      do_cache_insert_all(adapter_meta.repo, schema, entries)

      :telemetry.execute(
        [:etso, :internal, :write_cache_entry, schema],
        %{
          count: count
        },
        %{
          schema: schema,
          query_id: query_id
        }
      )

      entry_repo.insert(
        Etso.Cache.CachedQuery.new(%{
          query_id: query_id,
          params: params,
          expected_count: count,
          updated_at: System.os_time(:microsecond),
          times_accessed: 0,
          average_time_between_access: 0
        })
      )
    end
  end

  defp is_cached?(entry_repo, query_id, params) do
    query =
      from(cached_query in Etso.Cache.CachedQuery,
        where: cached_query.query_id == ^query_id and cached_query.params == ^params
      )

    case entry_repo.one(query) do
      nil -> false
      cached_query -> cached_query
    end
  end

  defp update_query_cache(
         %{repo: _repo, cache_for: nil},
         _,
         {:cache, update, {id, query_cache}},
         _
       ) do
    query_cache = Map.put(query_cache, :strategy, :cache_only)
    update.({id, query_cache})
    {id, query_cache}
  end

  defp update_query_cache(
         %{repo: _, cache_for: original_repo},
         query_meta,
         {:cache, update, {id, query_cache}},
         options
       ) do
    {adapter, _} = Ecto.Repo.Registry.lookup(original_repo)
    original_prepare = prepare_original_query(adapter, query_cache.type, query_cache.query)

    # we don't want update and delete queries cached
    query_strategy =
      if query_cache.type == :all do
        :cache_or_original
      else
        :cache_only
      end

    # allows to override the strategy
    strategy = Keyword.get(options, :strategy, query_strategy)

    query_cache =
      query_cache
      |> Map.put(:original_prepare, original_prepare)
      |> Map.put(:strategy, strategy)
      |> Map.put(:query_meta, query_meta)

    update.({id, query_cache})
    {id, query_cache}
  end

  defp update_query_cache(_, _, {:cached, _update, _reset, query_cache}, _) do
    query_cache
  end

  defp update_query_cache(_, _, {:nocache, {query_id, %{strategy: :cache_only}}} = query_cache, _) do
    query_cache
  end

  defp update_query_cache(%{cache_for: original_repo}, _, {:nocache, {query_id, exec}}, _) do
    {adapter, _} = Ecto.Repo.Registry.lookup(original_repo)
    original_prepare = prepare_original_query(adapter, exec.type, exec.query)

    exec =
      exec
      |> Map.put(:original_prepare, original_prepare)
      |> Map.put(:strategy, :original_only)

    {query_id, exec}
  end

  defp prepare_original_query(adapter, type, query) do
    case adapter.prepare(type, query) do
      {:cache, {id, original_query}} -> {:nocache, {id, original_query}}
      no_cache -> no_cache
    end
  end

  defp query_original(repo, query_meta, query_cache, params, options) do
    {adapter, adapter_meta} = Ecto.Repo.Registry.lookup(repo)
    adapter.execute(adapter_meta, query_meta, query_cache.original_prepare, params, options)
  end

  defp query_on_cache(repo, query_cache, params) do
    case query_cache.type do
      :all -> execute_on_cache(repo, query_cache.query, params)
      :update_all -> update_in_cache(repo, query_cache.query, params)
      :delete_all -> delete_from_cache(repo, query_cache.query, params)
    end
  end

  defp execute_on_cache(repo, query, params) do
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build_select(query, params)
    ets_objects = :ets.select(ets_table, [ets_match])
    {length(ets_objects), ets_objects}
  end

  defp delete_from_cache(repo, query, params) do
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build_delete(query, params)
    count = :ets.select_delete(ets_table, [ets_match])
    {count, nil}
  end

  defp update_in_cache(repo, query, params) do
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build_update(query, params)
    count = :ets.select_replace(ets_table, [ets_match])
    {count, nil}
  end

  def stream(%{repo: repo}, _, {:nocache, {_, query}}, params, options) do
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build_select(query, params)
    ets_limit = Keyword.get(options, :max_rows, 500)
    stream_start_fun = fn -> stream_start(ets_table, ets_match, ets_limit) end
    stream_next_fun = fn acc -> stream_next(acc) end
    stream_after_fun = fn acc -> stream_after(ets_table, acc) end
    Stream.resource(stream_start_fun, stream_next_fun, stream_after_fun)
  end

  defp stream_start(ets_table, ets_match, ets_limit) do
    :ets.safe_fixtable(ets_table, true)
    :ets.select(ets_table, [ets_match], ets_limit)
  end

  defp stream_next(:"$end_of_table") do
    {:halt, :ok}
  end

  defp stream_next({ets_objects, ets_continuation}) do
    {[{length(ets_objects), ets_objects}], :ets.select(ets_continuation)}
  end

  defp stream_after(ets_table, :ok) do
    :ets.safe_fixtable(ets_table, false)
  end

  defp stream_after(_, acc) do
    acc
  end

  defp do_cache_insert_all(repo, schema, entries) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_field_names = TableStructure.field_names(schema)
    ets_changes = TableStructure.entries_to_tuples(ets_field_names, entries)
    ets_result = :ets.insert(ets_table, ets_changes)
    if ets_result, do: {length(ets_changes), nil}, else: {0, nil}
  end

  defp get_schema_from_query(query) do
    elem(query.from.source, 1)
  end
end
