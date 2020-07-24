defmodule Etso.Adapter.Behaviour.Queryable do
  @moduledoc false

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.MatchSpecification
  alias Etso.ETS.TableStructure
  import Ecto.Query

  def prepare(type, query) do
    id = System.unique_integer([:positive])
    schema = get_schema_from_query(query)

    primary_key = schema.__schema__(:primary_key)

    {do_lookup, primary_key_filter} =
      case build_filter(query.wheres, []) do
        {:ok, filters} ->
          filter_keys =
            filters
            |> Keyword.keys()
            |> MapSet.new()

          if MapSet.subset?(MapSet.new(primary_key), filter_keys) do
            {true, take_ordered(filters, primary_key)}
          else
            {false, []}
          end

        _ ->
          {false, []}
      end

    query_cache = %{
      type: type,
      query: query,
      schema: schema,
      do_lookup: do_lookup,
      primary_key_filter: primary_key_filter
    }

    if MatchSpecification.is_supported?(query) do
      {:cache, {id, query_cache}}
    else
      {:nocache, {id, query_cache}}
    end
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

  defp write_resultset_in_cache(_, _, _, _, %{select: %{from: :none}}), do: nil

  defp write_resultset_in_cache(query_id, params, resultset, adapter_meta, query_meta) do
    {count, results} = resultset
    {:any, {:source, {_table, schema}, _prefix, _types}} = query_meta.select.from

    has_results? = count > 0
    insert_into_cache? = has_results?

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

  defp update_query_cache(_, _, {:nocache, {_, %{strategy: :cache_only}}} = query_cache, _) do
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
      :all ->
        if Map.get(query_cache, :do_lookup, false) do
          {_, schema} = query_cache.query.from.source
          primary_key = populate_filters(query_cache.primary_key_filter, params)
          lookup_in_cache(repo, schema, primary_key, query_cache.query.select.fields)
        else
          execute_on_cache(repo, query_cache.query, params)
        end

      :update_all ->
        update_in_cache(repo, query_cache.query, params)

      :delete_all ->
        delete_from_cache(repo, query_cache.query, params)
    end
  end

  defp lookup_in_cache(repo, schema, primary_key, select_fields) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)

    schema_fields = schema.__schema__(:fields)

    ets_objects =
      case primary_key do
        [{_key, value}] ->
          ets_table
          |> :ets.lookup(value)
          |> Enum.map(&conform_to_ecto(schema_fields, select_fields, &1))

        primary_key ->
          primary_key_tuple =
            primary_key
            |> Keyword.values()
            |> List.to_tuple()

          ets_table
          |> :ets.lookup(primary_key_tuple)
          |> Enum.map(&conform_to_ecto(schema_fields, select_fields, &1))
      end

    {length(ets_objects), ets_objects}
  end

  defp conform_to_ecto(schema_fields, select_fields, lookup_result) do
    values =
      case Tuple.to_list(lookup_result) do
        [tuple | rest] when is_tuple(tuple) -> Tuple.to_list(tuple) ++ rest
        result -> result
      end

    lookup = lookup_result(schema_fields, values, %{})

    Enum.map(select_fields, fn {{:., _, [{:&, [], [0]}, select_field]}, [], []} ->
      Map.get(lookup, select_field)
    end)
  end

  defp lookup_result([], [], result) do
    result
  end

  defp lookup_result([field_name | field_names], [field_value | values], result) do
    lookup_result(field_names, values, Map.put(result, field_name, field_value))
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

  defp build_filter([], filters) do
    {:ok, filters}
  end

  # first where clause does not matter if it is :or or :and
  defp build_filter([%{expr: expr} | rest], filters = []) do
    case build_filter(expr, filters) do
      {:ok, filters} -> build_filter(rest, filters)
      error -> error
    end
  end

  defp build_filter([%{expr: expr, op: :and} | rest], filters) do
    case build_filter(expr, filters) do
      {:ok, filters} -> build_filter(rest, filters)
      error -> error
    end
  end

  defp build_filter({:and, [], [lhs, rhs]}, filters) do
    case build_filter(lhs, filters) do
      {:ok, filters} -> build_filter(rhs, filters)
      error -> error
    end
  end

  defp build_filter({:==, [], [lhs, rhs]}, filters) do
    case build_filter(lhs) do
      atom when is_atom(atom) ->
        value = build_filter(rhs)
        filters = [{atom, value} | filters]
        {:ok, filters}

      _ ->
        {:error, :equals_expression_without_atom}
    end
  end

  defp build_filter(_, _) do
    {:error, :unsupported_subset}
  end

  defp build_filter({{:., [], [{:&, [], [0]}, field_name]}, [], []}) do
    field_name
  end

  defp build_filter({:^, [], [index]}) do
    {:param, index}
  end

  defp build_filter(value) when not is_tuple(value) do
    {:value, value}
  end

  defp populate_filters(filters, params) do
    Enum.map(filters, fn
      {attribute, {:value, value}} -> {attribute, value}
      {attribute, {:param, index}} -> {attribute, Enum.at(params, index)}
    end)
  end

  defp take_ordered(fields, field_names, results \\ [])

  defp take_ordered(fields, [field_name | field_names], result) do
    case Keyword.fetch(fields, field_name) do
      {:ok, value} -> take_ordered(fields, field_names, [{field_name, value} | result])
      :error -> result
    end
  end

  defp take_ordered(_, [], result) do
    Enum.reverse(result)
  end
end
