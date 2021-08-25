defmodule Postgrex.Utils do
  @moduledoc false

  @extensions [
    Postgrex.Extensions.Array,
    Postgrex.Extensions.BitString,
    Postgrex.Extensions.Bool,
    Postgrex.Extensions.Box,
    Postgrex.Extensions.Circle,
    Postgrex.Extensions.Date,
    Postgrex.Extensions.Float4,
    Postgrex.Extensions.Float8,
    Postgrex.Extensions.HStore,
    Postgrex.Extensions.INET,
    Postgrex.Extensions.Int2,
    Postgrex.Extensions.Int4,
    Postgrex.Extensions.Int8,
    Postgrex.Extensions.Interval,
    Postgrex.Extensions.JSON,
    Postgrex.Extensions.JSONB,
    Postgrex.Extensions.Line,
    Postgrex.Extensions.LineSegment,
    Postgrex.Extensions.MACADDR,
    Postgrex.Extensions.Name,
    Postgrex.Extensions.Numeric,
    Postgrex.Extensions.OID,
    Postgrex.Extensions.Path,
    Postgrex.Extensions.Point,
    Postgrex.Extensions.Polygon,
    Postgrex.Extensions.Range,
    Postgrex.Extensions.Raw,
    Postgrex.Extensions.Record,
    Postgrex.Extensions.TID,
    Postgrex.Extensions.Time,
    Postgrex.Extensions.Timestamp,
    Postgrex.Extensions.TimestampTZ,
    Postgrex.Extensions.TimeTZ,
    Postgrex.Extensions.TSVector,
    Postgrex.Extensions.UUID,
    Postgrex.Extensions.VoidBinary,
    Postgrex.Extensions.VoidText,
    Postgrex.Extensions.Xid8
  ]

  @doc """
  Checks if a given extension is a default extension.
  """
  for ext <- @extensions do
    def default_extension?(unquote(ext)), do: true
  end

  def default_extension?(_), do: false

  @doc """
  List all default extensions.
  """
  @spec default_extensions(Keyword.t()) :: [{module(), Keyword.t()}]
  def default_extensions(opts \\ []) do
    Enum.map(@extensions, &{&1, opts})
  end

  @doc """
  Converts pg major.minor.patch (http://www.postgresql.org/support/versioning) version to an integer
  """
  def parse_version(version) do
    segments =
      version
      |> String.split(" ", parts: 2)
      |> hd()
      |> String.split(".", parts: 4)
      |> Enum.map(&parse_version_bit/1)

    case segments do
      [major, minor, patch, _] -> {major, minor, patch}
      [major, minor, patch] -> {major, minor, patch}
      [major, minor] -> {major, minor, 0}
      [major] -> {major, 0, 0}
    end
  end

  @doc """
  Fills in the given `opts` with default options.
  """
  @spec default_opts(Keyword.t()) :: Keyword.t()
  def default_opts(opts) do
    opts
    |> Keyword.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
    |> Keyword.put_new(:password, System.get_env("PGPASSWORD"))
    |> Keyword.put_new(:database, System.get_env("PGDATABASE"))
    |> Keyword.put_new(:hostname, System.get_env("PGHOST") || "localhost")
    |> Keyword.put_new(:port, System.get_env("PGPORT"))
    |> Keyword.update!(:port, &normalize_port/1)
    |> Keyword.put_new(:types, Postgrex.DefaultTypes)
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp normalize_port(port) when is_binary(port), do: String.to_integer(port)
  defp normalize_port(port), do: port

  @doc """
  Return encode error message.
  """
  def encode_msg(%Postgrex.TypeInfo{type: type}, observed, expected) do
    "Postgrex expected #{to_desc(expected)} that can be encoded/cast to " <>
      "type #{inspect(type)}, got #{inspect(observed)}. Please make sure the " <>
      "value you are passing matches the definition in your table or in your " <>
      "query or convert the value accordingly."
  end

  @doc """
  Return encode error message.
  """
  def encode_msg(%Date{calendar: calendar} = observed, _expected) when calendar != Calendar.ISO do
    "Postgrex expected a %Date{} in the `Calendar.ISO` calendar, got #{inspect(observed)}. " <>
      "Postgrex (and PostgreSQL) support dates in the `Calendar.ISO` calendar only."
  end

  def encode_msg(%NaiveDateTime{calendar: calendar} = observed, _expected)
      when calendar != Calendar.ISO do
    "Postgrex expected a %NaiveDateTime{} in the `Calendar.ISO` calendar, got #{inspect(observed)}. " <>
      "Postgrex (and PostgreSQL) support naive datetimes in the `Calendar.ISO` calendar only."
  end

  def encode_msg(%DateTime{calendar: calendar} = observed, _expected)
      when calendar != Calendar.ISO do
    "Postgrex expected a %DateTime{} in the `Calendar.ISO` calendar, got #{inspect(observed)}. " <>
      "Postgrex (and PostgreSQL) support datetimes in the `Calendar.ISO` calendar only."
  end

  def encode_msg(%Time{calendar: calendar} = observed, _expected) when calendar != Calendar.ISO do
    "Postgrex expected a %Time{} in the `Calendar.ISO` calendar, got #{inspect(observed)}. " <>
      "Postgrex (and PostgreSQL) support times in the `Calendar.ISO` calendar only."
  end

  def encode_msg(observed, expected) do
    "Postgrex expected #{to_desc(expected)}, got #{inspect(observed)}. " <>
      "Please make sure the value you are passing matches the definition in " <>
      "your table or in your query or convert the value accordingly."
  end

  @doc """
  Return type error message.
  """
  def type_msg(%Postgrex.TypeInfo{type: json}, module)
      when json in ["json", "jsonb"] do
    "type `#{json}` can not be handled by the types module #{inspect(module)}, " <>
      "it must define a `:json` library in its options to support JSON types"
  end

  def type_msg(%Postgrex.TypeInfo{type: type}, module) do
    "type `#{type}` can not be handled by the types module #{inspect(module)}"
  end

  ## Helpers

  defp parse_version_bit(bit) do
    {int, _} = Integer.parse(bit)
    int
  end

  defp to_desc(struct) when is_atom(struct), do: "%#{inspect(struct)}{}"
  defp to_desc(%Range{} = range), do: "an integer in #{inspect(range)}"
  defp to_desc({a, b}), do: to_desc(a) <> " or " <> to_desc(b)
  defp to_desc(desc) when is_binary(desc), do: desc
end
