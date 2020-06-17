defmodule Etso.ETS.TableStructure do
  @moduledoc """
  The ETS Table Structure module contains various convenience functions to aid the transformation
  between Ecto Schemas (maps) and ETS entries (tuples). The primary key is moved to the head, in
  accordance with ETS conventions. Composite primary keys can not be accepted, however.
  """

  def field_names(schema) do
    fields = schema.__schema__(:fields)

    case schema.__schema__(:primary_key) do
      [_] = primary_key ->
        primary_key ++ (fields -- primary_key)

      primary_key ->
        [primary_key] ++ (fields -- primary_key)
    end
  end

  def fields_to_tuple(field_names, fields) do
    field_names
    |> Enum.map(fn
      field_name when is_atom(field_name) ->
        Keyword.get(fields, field_name, nil)

      composite_fieldname when is_list(composite_fieldname) ->
        fields
        |> take_ordered(composite_fieldname)
        |> Keyword.values()
        |> List.to_tuple()
    end)
    |> List.to_tuple()
  end

  defp take_ordered(fields, field_names, results \\ [])

  defp take_ordered(fields, [field_name | field_names], result) do
    {:ok, value} = Keyword.fetch(fields, field_name)
    take_ordered(fields, field_names, [{field_name, value} | result])
  end

  defp take_ordered(_, [], result) do
    Enum.reverse(result)
  end

  def entries_to_tuples(field_names, entries) do
    for entry <- entries do
      fields_to_tuple(field_names, entry)
    end
  end
end
