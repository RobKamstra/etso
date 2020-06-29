defmodule Etso.Type.Term do
  @moduledoc """
  Custom ecto field type for Elixir terms.
  """

  @behaviour Ecto.Type

  def type, do: :term

  def cast(value), do: {:ok, value}

  def load(value), do: {:ok, value}

  def dump(value), do: {:ok, value}

  def embed_as(_format), do: :self

  def equal?(term1, term2), do: term1 == term2
end
