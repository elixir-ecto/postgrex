defmodule Postgrex.Utils do
  @moduledoc false

  @extensions [
    Postgrex.Extensions.Array,
    Postgrex.Extensions.Bool,
    Postgrex.Extensions.Date,
    Postgrex.Extensions.Float4,
    Postgrex.Extensions.Float8,
    Postgrex.Extensions.HStore,
    Postgrex.Extensions.Int2,
    Postgrex.Extensions.Int4,
    Postgrex.Extensions.Int8,
    Postgrex.Extensions.Interval,
    Postgrex.Extensions.MACADDR,
    Postgrex.Extensions.Network,
    Postgrex.Extensions.Numeric,
    Postgrex.Extensions.OID,
    Postgrex.Extensions.Point,
    Postgrex.Extensions.Range,
    Postgrex.Extensions.Raw,
    Postgrex.Extensions.Record,
    Postgrex.Extensions.TID,
    Postgrex.Extensions.Time,
    Postgrex.Extensions.Timestamp,
    Postgrex.Extensions.Interval,
    Postgrex.Extensions.UUID,
    Postgrex.Extensions.Void]

  @doc """
  Converts pg major.minor.patch (http://www.postgresql.org/support/versioning) version to an integer
  """
  def parse_version(version) do
    list =
      version
      |> String.split(".")
      |> Enum.map(&elem(Integer.parse(&1), 0))

    case list do
      [major, minor, patch] -> {major, minor, patch}
      [major, minor] -> {major, minor, 0}
      [major] -> {major, 0, 0}
    end
  end

  @doc """
  Fills in the given `opts` with default options.
  """
  @spec default_opts(Keyword.t) :: Keyword.t
  def default_opts(opts) do
    opts
    |> Keyword.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
    |> Keyword.put_new(:password, System.get_env("PGPASSWORD"))
    |> Keyword.put_new(:hostname, System.get_env("PGHOST") || "localhost")
    |> Keyword.update(:port, normalize_port(System.get_env("PGPORT")), &normalize_port/1)
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp normalize_port(port) when is_binary(port), do: String.to_integer(port)
  defp normalize_port(port), do: port

  @doc """
  List all default extensions.
  """
  @spec default_extensions(Keyword.t) :: [{module(), Keyword.t}]
  def default_extensions(opts \\ []) do
    Enum.map(@extensions, &{&1, opts})
  end

  @doc """
  Return encode error message.
  """
  def encode_msg(%Postgrex.TypeInfo{type: type}, observed, expected) do
    "Postgrex expected #{to_desc(expected)} that can be encoded/cast to " <>
    "type #{inspect type}, got #{inspect observed}. Please make sure the " <>
    "value you are passing matches the definition in your table or in your " <>
    "query or convert the value accordingly."
  end

  ## Helpers

  defp to_desc(struct) when is_atom(struct), do: "%#{inspect struct}{}"
  defp to_desc(%Range{} = range), do: "an integer in #{inspect range}"
  defp to_desc({a, b}), do: to_desc(a) <> " or " <> to_desc(b)
  defp to_desc(desc) when is_binary(desc), do: desc
end
