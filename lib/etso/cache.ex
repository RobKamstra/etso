defmodule Etso.Cache do
  defmacro __using__(opts) do
    quote do
      use Ecto.Repo, unquote(opts)

      defmodule Cache do
        use Ecto.Repo, unquote(opts)
      end
    end
  end
end
