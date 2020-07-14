defmodule Etso.ETS.MatchSpecification do
  @moduledoc """
  The ETS Match Specifications module contains various functions which convert Ecto queries to
  ETS Match Specifications in order to execute the given queries.
  """
  def is_supported?(%Ecto.Query{} = query) do
    is_supported?(query.wheres)
  end

  def is_supported?([]), do: true

  def is_supported?([%Ecto.Query.BooleanExpr{expr: expression} | rest]) do
    is_supported?(expression) and is_supported?(rest)
  end

  def is_supported?({:not, [], [clause]}) do
    is_supported?(clause)
  end

  for operator <- ~w(and or)a do
    def is_supported?({unquote(operator), [], [lhs, rhs]}) do
      is_supported?(lhs) and is_supported?(rhs)
    end
  end

  for operator <- ~w(== != < > <= >= in)a do
    def is_supported?({unquote(operator), _, _}) do
      true
    end
  end

  def is_supported?(_) do
    false
  end

  def build_select(query, params) do
    {_, schema} = query.from.source
    field_names = Etso.ETS.TableStructure.field_names(schema)
    match_head = build_head(field_names)
    match_conditions = build_conditions(field_names, params, query.wheres)
    match_body = [build_body(field_names, query.select.fields)]

    {match_head, match_conditions, match_body}
  end

  def build_delete(query, params) do
    {_, schema} = query.from.source
    field_names = Etso.ETS.TableStructure.field_names(schema)
    match_head = build_head(field_names)
    match_conditions = build_conditions(field_names, params, query.wheres)
    match_body = [true]

    {match_head, match_conditions, match_body}
  end

  def build_update(query, params) do
    {_, schema} = query.from.source
    field_names = Etso.ETS.TableStructure.field_names(schema)
    match_head = build_head(field_names)
    match_conditions = build_conditions(field_names, params, query.wheres)
    match_body = [build_update_body(field_names, query.updates, params)]

    {match_head, match_conditions, match_body}
  end

  def build(%{updates: []} = query, params) do
    {_, schema} = query.from.source
    field_names = Etso.ETS.TableStructure.field_names(schema)

    match_head = build_head(field_names)
    match_conditions = build_conditions(field_names, params, query.wheres)

    match_body =
      if query.select do
        [build_body(field_names, query.select.fields)]
      else
        [true]
      end

    {match_head, match_conditions, match_body}
  end

  def build(query, params) do
    {_, schema} = query.from.source
    field_names = Etso.ETS.TableStructure.field_names(schema)

    match_head = build_head(field_names)
    match_conditions = build_conditions(field_names, params, query.wheres)
    match_body = [build_update_body(field_names, query.updates, params)]
    {match_head, match_conditions, match_body}
  end

  defp build_head(field_names) do
    {field_indexes, _} = Enum.reduce(field_names, {[], 1}, &build_head_selection/2)
    List.to_tuple(field_indexes)
  end

  defp build_head_selection(field_name, {field_indexes, index}) when is_atom(field_name) do
    field_index = [:"$#{index}"]
    {field_indexes ++ field_index, index + 1}
  end

  defp build_head_selection(composite_names, {field_indexes, index}) do
    count = length(composite_names)

    tuple =
      index..count
      |> Enum.map(fn x -> :"$#{x}" end)
      |> List.to_tuple()
      |> List.wrap()

    {field_indexes ++ tuple, index + count}
  end

  defp build_conditions(_field_names, _params, []) do
    []
  end

  defp build_conditions(field_names, params, query_wheres) do
    [%{expr: expression} | rest] = query_wheres
    initial_condition = build_condition(field_names, params, expression)

    rest
    |> Enum.reduce(initial_condition, fn %Ecto.Query.BooleanExpr{expr: expression, op: op}, acc ->
      case op do
        :and -> {:andalso, acc, build_condition(field_names, params, expression)}
        :or -> {:orelse, acc, build_condition(field_names, params, expression)}
      end
    end)
    |> List.wrap()
  end

  defmacrop guard_operator(:and), do: :andalso
  defmacrop guard_operator(:or), do: :orelse
  defmacrop guard_operator(:!=), do: :"/="
  defmacrop guard_operator(:<=), do: :"=<"
  defmacrop guard_operator(operator), do: operator

  for operator <- ~w(== != < > <= >= and or)a do
    defp build_condition(field_names, params, {unquote(operator), [], [lhs, rhs]}) do
      lhs_condition = build_condition(field_names, params, lhs)
      rhs_condition = build_condition(field_names, params, rhs)
      {guard_operator(unquote(operator)), lhs_condition, rhs_condition}
    end
  end

  for operator <- ~w(not)a do
    defp build_condition(field_names, params, {unquote(operator), [], [clause]}) do
      condition = build_condition(field_names, params, clause)
      {guard_operator(unquote(operator)), condition}
    end
  end

  defp build_condition(field_names, params, {:in, [], [field, value]}) do
    field_name = resolve_field_name(field)
    field_index = get_field_index(field_names, field_name)

    resolve_field_values(params, value)
    |> Enum.map(&{:==, :"$#{field_index}", &1})
    |> Enum.reduce(&{:orelse, &1, &2})
  end

  defp build_condition(field_names, _, {{:., [], [{:&, [], [0]}, field_name]}, [], []}) do
    :"$#{get_field_index(field_names, field_name)}"
  end

  defp build_condition(_, params, {:^, [], [index]}) do
    Enum.at(params, index)
  end

  defp build_condition(_, _, value) when not is_tuple(value) do
    value
  end

  defp build_body(field_names, query_select_fields) do
    for select_field <- query_select_fields do
      field_name = resolve_field_name(select_field)
      field_index = get_field_index(field_names, field_name)
      :"$#{field_index}"
    end
  end

  defp build_update_body(field_names, update_expressions, params) do
    updates = updates(update_expressions, params, field_names)

    match_spec_updates =
      for field_name <- field_names,
          field_index = get_field_index(field_names, field_name) do
        field_index = :"$#{field_index}"

        case Map.get(updates, field_name, nil) do
          # no update
          nil -> field_index
          {:inc, data} -> {:+, field_index, data}
          {:set, data} -> data
        end
      end

    {List.to_tuple(match_spec_updates)}
  end

  defp updates(update_expressions, params, field_names) do
    for query_expression <- update_expressions,
        update_expression = query_expression.expr,
        {update_type, update} <- update_expression,
        {update_field_name, field_update_expression} <- update,
        into: %{} do
      update_value = update(field_update_expression, params, field_names)
      {update_field_name, {update_type, update_value}}
    end
  end

  defp update({:+, [], [lhs, rhs]}, params, field_names) do
    {:+, update(lhs, params, field_names), update(rhs, params, field_names)}
  end

  defp update({:*, [], [lhs, rhs]}, params, field_names) do
    {:*, update(lhs, params, field_names), update(rhs, params, field_names)}
  end

  defp update({:-, [], [lhs, rhs]}, params, field_names) do
    {:-, update(lhs, params, field_names), update(rhs, params, field_names)}
  end

  defp update({{:., [], [{:&, [], [0]}, field_name]}, [], []}, _params, field_names)
       when is_atom(field_name) do
    field_index = get_field_index(field_names, field_name)
    :"$#{field_index}"
  end

  defp update({:^, [], [index]}, params, _field_names) do
    Enum.at(params, index)
  end

  defp update(value, _params, _field_names) do
    value
  end

  defp resolve_field_name(field) do
    {{:., _, [{:&, [], [0]}, field_name]}, [], []} = field
    field_name
  end

  defp resolve_field_values(params, {:^, [], [start, stop]}) do
    for index <- start..(stop - 1) do
      Enum.at(params, index)
    end
  end

  defp resolve_field_values(params, {:^, [], indices}) do
    for index <- indices do
      Enum.at(params, index)
    end
  end

  defp resolve_field_values(params, value) when is_list(value) do
    Enum.flat_map(value, &resolve_field_values(params, &1))
  end

  defp resolve_field_values(_params, value) do
    List.wrap(value)
  end

  defp get_field_index(field_names, field_name) do
    Enum.reduce_while(field_names, 1, fn
      x, index when is_atom(x) ->
        if x == field_name do
          {:halt, index}
        else
          {:cont, index + 1}
        end

      x, index when is_list(x) ->
        result = Enum.find_index(x, fn y -> y == field_name end)

        if result do
          {:halt, index + result}
        else
          {:cont, index + Enum.count(x)}
        end
    end)
  end
end
