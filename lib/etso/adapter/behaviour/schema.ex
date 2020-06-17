defmodule Etso.Adapter.Behaviour.Schema do
  @moduledoc false

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.TableStructure

  def autogenerate(:id), do: :erlang.unique_integer()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()
  def autogenerate(:embed_id), do: Ecto.UUID.bingenerate()

  def insert_all(%{repo: repo}, %{schema: schema}, _, entries, _, _, _) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_field_names = TableStructure.field_names(schema)
    ets_changes = TableStructure.entries_to_tuples(ets_field_names, entries)
    ets_result = :ets.insert(ets_table, ets_changes)
    if ets_result, do: {length(ets_changes), nil}, else: {0, nil}
  end

  def insert(%{repo: repo}, %{schema: schema}, fields, _, _, _) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_field_names = TableStructure.field_names(schema)
    ets_changes = TableStructure.fields_to_tuple(ets_field_names, fields)
    ets_result = :ets.insert(ets_table, ets_changes)
    if ets_result, do: {:ok, []}, else: {:invalid, []}
  end

  def update(%{repo: repo}, %{schema: schema}, fields, filters, [], _) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    primary_key = schema.__schema__(:primary_key)

    case take_ordered(fields, primary_key) do
      # the update is not about any primary keys
      [] ->
        key = get_primary_key(filters, primary_key)
        ets_updates = build_ets_updates(schema, fields)
        ets_result = :ets.update_element(ets_table, key, ets_updates)
        if ets_result, do: {:ok, []}, else: {:error, :stale}

      # the update contains primary key values
      _ ->
        key = get_primary_key(filters, primary_key)
        [original] = :ets.lookup(ets_table, key)
        new_entry = build_insert(original, schema, fields)
        ets_result = :ets.insert(ets_table, new_entry)

        if ets_result do
          :ets.delete(ets_table, key)
          {:ok, []}
        else
          {:invalid, []}
        end
    end
  end

  def delete(%{repo: repo}, %{schema: schema}, filters, _) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    primary_key = schema.__schema__(:primary_key)
    key = get_primary_key(filters, primary_key)
    :ets.delete(ets_table, key)
    {:ok, []}
  end

  def lookup(%{repo: repo, cache_for: nil}, %{schema: schema}, primary_key) do
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    :ets.lookup(ets_table, primary_key)
  end

  defp get_primary_key(filters, primary_key) do
    filters
    |> take_ordered(primary_key)
    |> Keyword.values()
    |> case do
      [key] -> key
      keys -> List.to_tuple(keys)
    end
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

  defp build_insert(original_tuple, schema, fields) do
    ets_field_names = TableStructure.field_names(schema)

    Enum.reduce(fields, original_tuple, &field_update(ets_field_names, &1, &2))
  end

  defp field_update(field_names, {field_name, field_value}, new_tuple) do
    Enum.reduce_while(field_names, {1, new_tuple}, fn
      x, {index, new_tuple} when is_list(x) ->
        pos_function = fn y -> y == field_name end

        case Enum.find_index(x, pos_function) do
          nil ->
            {:cont, {index + 1, new_tuple}}

          found_index ->
            original_key = elem(new_tuple, index - 1)
            new_key = :erlang.setelement(found_index + 1, original_key, field_value)
            result = :erlang.setelement(index, new_tuple, new_key)
            {:halt, result}
        end

      x, {index, new_tuple} ->
        if x == field_name do
          result = :erlang.setelement(index, new_tuple, field_value)
          {:halt, result}
        else
          {:cont, {index + 1, new_tuple}}
        end
    end)
  end

  defp build_ets_updates(schema, fields) do
    ets_field_names = TableStructure.field_names(schema)

    for {field_name, field_value} <- fields do
      position_fun = fn x -> x == field_name end
      position = 1 + Enum.find_index(ets_field_names, position_fun)
      {position, field_value}
    end
  end
end
