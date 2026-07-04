defmodule Postgrex.SCRAM do
  @moduledoc false

  alias Postgrex.SCRAM

  @hash_length 32
  @nonce_length 24
  @nonce_rand_bytes div(@nonce_length * 6, 8)
  @cb_gs2_header "p=tls-server-end-point,,"

  # Chooses the SCRAM mechanism and gs2 header from the mechanisms the server offered.
  def negotiate_channel_binding(server_mechanisms, sock, mode) do
    offered = parse_mechanisms(server_mechanisms)
    plus_offered? = "SCRAM-SHA-256-PLUS" in offered
    tls? = match?({:ssl, _}, sock)

    cond do
      mode == :disable ->
        {:ok, {"SCRAM-SHA-256", "n,,", ""}}

      tls? and plus_offered? ->
        case cert_hash(sock) do
          {:ok, hash} ->
            {:ok, {"SCRAM-SHA-256-PLUS", @cb_gs2_header, hash}}

          :error when mode == :require ->
            {:error,
             %Postgrex.Error{
               message:
                 "channel binding is required but the server certificate has no usable hash"
             }}

          # Fallback
          :error ->
            {:ok, {"SCRAM-SHA-256", "n,,", ""}}
        end

      mode == :require ->
        {:error,
         %Postgrex.Error{
           message:
             "channel binding is required but not offered by the server, or the connection is not using SSL"
         }}

      true ->
        cbind_flag = if tls?, do: "y,,", else: "n,,"
        {:ok, {"SCRAM-SHA-256", cbind_flag, ""}}
    end
  end

  def client_first({mechanism, gs2_header, _cbind_data}) do
    nonce = @nonce_rand_bytes |> :crypto.strong_rand_bytes() |> Base.encode64()
    sasl_data = [gs2_header, "n=,r=", nonce]
    size = <<IO.iodata_length(sasl_data)::signed-size(32)>>
    [mechanism, 0, size, sasl_data]
  end

  def client_final(data, {_mechanism, gs2_header, cbind_data}, opts) do
    # Extract data from server-first message
    server = parse_server_data(data)
    {:ok, server_s} = Base.decode64(server[?s])
    server_i = String.to_integer(server[?i])

    # Create and cache client and server keys if they don't already exist
    pass = Keyword.fetch!(opts, :password)
    cache_key = create_cache_key(pass, server_s, server_i)

    {client_key, _server_key} =
      SCRAM.LockedCache.run(cache_key, fn ->
        calculate_client_server_keys(pass, server_s, server_i)
      end)

    # Construct client signature and proof
    cbind_input = Base.encode64(IO.iodata_to_binary([gs2_header, cbind_data]))
    message_without_proof = ["c=", cbind_input, ",r=", server[?r]]
    client_nonce = binary_part(server[?r], 0, @nonce_length)
    message = ["n=,r=", client_nonce, ",r=", server[?r], ",s=", server[?s], ",i=", server[?i], ?,]
    auth_message = IO.iodata_to_binary([message | message_without_proof])

    client_sig = hmac(:sha256, :crypto.hash(:sha256, client_key), auth_message)
    proof = Base.encode64(:crypto.exor(client_key, client_sig))

    # Store data needed to verify the server signature
    scram_state = %{salt: server_s, iterations: server_i, auth_message: auth_message}

    {[message_without_proof, ",p=", proof], scram_state}
  end

  def verify_server(data, scram_state, opts) do
    data
    |> parse_server_data()
    |> do_verify_server(scram_state, opts)
  end

  defp parse_mechanisms(data) do
    data |> :binary.split(<<0>>, [:global]) |> Enum.reject(&(&1 == ""))
  end

  # RFC 5929 tls-server-end-point
  defp cert_hash({:ssl, sslsock}) do
    with {:ok, der} <- :ssl.peercert(sslsock),
         {:Certificate, _tbs, sig_alg, _sig} <- :public_key.pkix_decode_cert(der, :plain),
         {_tag, oid, _params} <- sig_alg,
         {digest, _sign} when digest != :none <- pkix_sign_type(oid) do
      # Ref: https://www.rfc-editor.org/info/rfc5929/#section-4.1
      digest = if digest in [:md5, :sha], do: :sha256, else: digest
      {:ok, :crypto.hash(digest, der)}
    else
      _ -> :error
    end
  end

  defp pkix_sign_type(oid) do
    :public_key.pkix_sign_types(oid)
  rescue
    _ -> {:none, :unknown}
  end

  defp do_verify_server(%{?e => server_e}, _scram_state, _opts) do
    msg = "error received in SCRAM server final message: #{inspect(server_e)}"
    {:error, %Postgrex.Error{message: msg}}
  end

  defp do_verify_server(%{?v => server_v}, scram_state, opts) do
    # Decode server signature from the server-final message
    {:ok, server_sig} = Base.decode64(server_v)

    # Construct expected server signature
    pass = Keyword.fetch!(opts, :password)
    cache_key = create_cache_key(pass, scram_state.salt, scram_state.iterations)
    {_client_key, server_key} = SCRAM.LockedCache.get(cache_key)
    expected_server_sig = hmac(:sha256, server_key, scram_state.auth_message)

    # Verify the server signature sent to us is correct
    if expected_server_sig == server_sig do
      :ok
    else
      msg = "cannot verify SCRAM server signature"
      {:error, %Postgrex.Error{message: msg}}
    end
  end

  defp do_verify_server(server, _scram_state, _opts) do
    msg = "unsupported SCRAM server final message: #{inspect(server)}"
    {:error, %Postgrex.Error{message: msg}}
  end

  defp parse_server_data(data) do
    for kv <- :binary.split(data, ",", [:global]), into: %{} do
      <<k, "=", v::binary>> = kv
      {k, v}
    end
  end

  defp create_cache_key(pass, salt, iterations) do
    {:crypto.hash(:sha256, pass), salt, iterations}
  end

  defp calculate_client_server_keys(pass, salt, iterations) do
    salted_pass = hash_password(pass, salt, iterations)
    client_key = hmac(:sha256, salted_pass, "Client Key")
    server_key = hmac(:sha256, salted_pass, "Server Key")

    {client_key, server_key}
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
    initial = hmac(:sha256, secret, <<salt::binary, block_index::integer-size(32)>>)
    block = iterate(secret, iterations - 1, initial, initial)
    length = byte_size(block) + length
    hash_password(secret, salt, iterations, block_index + 1, [acc | block], length)
  end

  defp iterate(_secret, 0, _prev, acc), do: acc

  defp iterate(secret, iteration, prev, acc) do
    next = hmac(:sha256, secret, prev)
    iterate(secret, iteration - 1, next, :crypto.exor(next, acc))
  end

  # :crypto.mac/4 was added in OTP-22.1, and :crypto.hmac/3 removed in OTP-24.
  # Check which function to use at compile time to avoid doing a round-trip
  # to the code server on every call. The downside is this module won't work
  # if it's compiled on OTP-22.0 or older then executed on OTP-24 or newer.
  if Code.ensure_loaded?(:crypto) and function_exported?(:crypto, :mac, 4) do
    defp hmac(type, key, data), do: :crypto.mac(:hmac, type, key, data)
  else
    defp hmac(type, key, data), do: :crypto.hmac(type, key, data)
  end
end
