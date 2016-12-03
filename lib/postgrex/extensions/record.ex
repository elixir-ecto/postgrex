defmodule Postgrex.Extensions.Record do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  @behaviour Postgrex.SuperExtension

  def init(_), do: nil

  def matching(_),
    do: [send: "record_send"]

  def format(_),
    do: :super_binary

  def oids(%Postgrex.TypeInfo{comp_elems: []}, _),
    do: nil
  def oids(%Postgrex.TypeInfo{comp_elems: comp_oids}, _),
    do: comp_oids

  def encode(_) do
    quote location: :keep do
      tuple, oids, types when is_tuple(tuple) ->
        # encode_tuple/3 defined by TypeModule
        case encode_tuple(tuple, oids, types) do
          :error ->
            raise ArgumentError,
              "expected a tuple of size #{length(oids)}, got: #{inspect tuple}"
          data ->
            [<<IO.iodata_length(data)+4::int32, tuple_size(tuple)::int32>> |
              data]
        end
      other, _, _ ->
        raise ArgumentError,
          Postgrex.Utils.encode_msg(other, "a tuple")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len::int32, binary::binary-size(len)>>, nil, types ->
        <<count::int32, data::binary>> = binary
        # decode_tuple/3 defined by TypeModule
        decode_tuple(data, count, types)
      <<len::int32, binary::binary-size(len)>>, oids, types ->
        <<_::int32, data::binary>> = binary
        # decode_tuple/3 defined by TypeModule
        decode_tuple(data, oids, types)
    end
  end
end
