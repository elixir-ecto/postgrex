defmodule Postgrex.SCRAM do
  @moduledoc false

  @hash_length 32
  @nonce_length 24
  @nonce_rand_bytes div(@nonce_length * 6, 8)
  @nonce_prefix "n,,n=,r="
  @nonce_encoded_size <<byte_size(@nonce_prefix) + @nonce_length :: signed-size(32)>>

  def challenge do
    nonce = @nonce_rand_bytes |> :crypto.strong_rand_bytes() |> Base.encode64()
    ["SCRAM-SHA-256", 0, @nonce_encoded_size, @nonce_prefix, nonce]
  end

  def verify(data, opts) do
    server =
      for kv <- :binary.split(data, ",", [:global]), into: %{} do
        <<k, "=", v::binary>> = kv
        {k, v}
      end

    {:ok, server_s} = Base.decode64(server[?s])
    server_i = String.to_integer(server[?i])

    pass = Keyword.fetch!(opts, :password)
    salted_pass = hash_password(pass, server_s, server_i)

    client_key = :crypto.hmac(:sha256, salted_pass, "Client Key")
    client_nonce = binary_part(server[?r], 0, @nonce_length)

    message = ["n=,r=", client_nonce, ",r=", server[?r], ",s=", server[?s], ",i=", server[?i], ?,]
    message_without_proof = ["c=biws,r=", server[?r]]

    auth_message = IO.iodata_to_binary([message | message_without_proof])
    client_sig = :crypto.hmac(:sha256, :crypto.hash(:sha256, client_key), auth_message)
    proof = Base.encode64(:crypto.exor(client_key, client_sig))
    [message_without_proof, ",p=", proof]
  end

  defp hash_password(secret, salt, iterations) do
    hash_password(secret, salt, iterations, 1, [], 0)
  end

  defp hash_password(_secret, _salt, _iterations, _block_index, acc, length)
       when length >= @hash_length do
    acc
    |> IO.iodata_to_binary()
    |> binary_part(0, @hash_length)
  end

  defp hash_password(secret, salt, iterations, block_index, acc, length) do
    initial = :crypto.hmac(:sha256, secret, <<salt::binary, block_index::integer-size(32)>>)
    block = iterate(secret, iterations - 1, initial, initial)
    length = byte_size(block) + length
    hash_password(secret, salt, iterations, block_index + 1, [acc | block], length)
  end

  defp iterate(_secret, 0, _prev, acc), do: acc

  defp iterate(secret, iteration, prev, acc) do
    next = :crypto.hmac(:sha256, secret, prev)
    iterate(secret, iteration - 1, next, :crypto.exor(next, acc))
  end
end
