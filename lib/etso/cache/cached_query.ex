defmodule Etso.Cache.CachedQuery do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  @required_fields [:query_id, :params, :expected_count]
  @optional_fields [:updated_at, :times_accessed, :average_time_between_access, :expires_at]

  schema "schemas" do
    field(:query_id, :integer, primary_key: true)
    field(:params, Etso.Type.Term, primary_key: true)
    field(:expected_count, Etso.Type.Term)
    field(:updated_at, :integer)
    field(:times_accessed, :integer)
    field(:average_time_between_access, :integer)
    field(:expires_at, :integer)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
  end

  def new(params) do
    changeset(%__MODULE__{}, params)
  end
end
