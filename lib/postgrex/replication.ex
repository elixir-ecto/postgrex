defmodule Postgrex.Replication do
  @moduledoc ~S"""
  A process that receives and sends PostgreSQL replication messages.

  > Note: this module is experimental and provides limited functionality.
  > We are glad to discuss and receive pull requests that extends the
  > scope of the module.

  ## Logical replication

  Let's see how to use this module for connecting to PostgreSQL
  for logical replication. First of all, you need to configure the
  wal level in PostgreSQL to logical. Run this inside your PostgreSQL
  shell/configuration:

      ALTER SYSTEM SET wal_level='logical';
      ALTER SYSTEM SET max_wal_senders='10';
      ALTER SYSTEM SET max_replication_slots='10';

  Then **you must restart your server**. Alternatively, you can set
  those values when starting "postgres". This is useful, for example,
  when running it from Docker:

      services:
        postgres:
          image: postgres:14
          env:
            ...
          command: ["postgres", "-c", "wal_level=logical"]

  For CI, GitHub Actions do not support setting command, so you can
  update and restart Postgres instead in a step:

      - name: "Set PG settings"
        run: |
          docker exec ${{ job.services.postgres.id }} sh -c 'echo "wal_level=logical" >> /var/lib/postgresql/data/postgresql.conf'
          docker restart ${{ job.services.pg.id }}

  Then you must create a publication to be replicated.
  This can be done in any session:

      CREATE PUBLICATION example FOR ALL TABLES;

  You can also filter if you want to publish insert, update,
  delete or a subset of them:

      # Skips updates (keeps inserts, deletes, begins, commits, etc)
      create PUBLICATION example FOR ALL TABLES WITH (publish = 'insert,delete');

      # Skips inserts, updates, and deletes (keeps begins, commits, etc)
      create PUBLICATION example FOR ALL TABLES WITH (publish = '');

  Now we are ready to create module that starts a replication slot
  and listens to our publication. Our example will use the pgoutput
  for logical replication and print all incoming messages to the
  terminal:

      Mix.install([:postgrex])

      defmodule Repl do
        use Postgrex.Replication

        def start_link(opts) do
          # Automatically reconnect if we lose connection.
          extra_opts = [
            auto_reconnect: true
          ]

          Postgrex.Replication.start_link(__MODULE__, :ok, extra_opts ++ opts)
        end

        @impl true
        def init(:ok) do
          send(self(), :create_slot)
          {:ok, %{}}
        end

        @impl true
        def handle_info(:create_slot, state) do
          query = "CREATE_REPLICATION_SLOT postgrex TEMPORARY LOGICAL pg_output NOEXPORT_SNAPSHOT"
          {:query, query, state}
        end

        @impl true
        def handle_result(_result, state) do
          {:noreply, state}
        end

        @impl true
        # https://www.postgresql.org/docs/14/protocol-replication.html
        def handle_data(<<?w, _wal_start::64, _wal_end::64, _clock::64, rest::binary>>, state) do
          IO.inspect(rest)
          {:noreply, [], state}
        end

        def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
          messages =
            case reply do
              1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
              0 -> []
            end

          {:noreply, messages, state}
        end

        @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
        defp current_time(), do: System.os_time(:microsecond) - @epoch
      end

      {:ok, pid} =
        Repl.start_link(
          host: "localhost",
          database: "demo_dev",
          username: "postgres",
        )

      Postgrex.Replication.start_replication(
        pid,
        "postgrex",
        plugin_opts: [proto_version: 1, publication_names: "postgrex_example"]
      )

      Process.sleep(:infinity)

  ## `use` options

  `use Postgrex.Replication` accepts a list of options which configures the
  child specification and therefore how it runs under a supervisor.
  The generated `child_spec/1` can be customized with the following options:

    * `:id` - the child specification identifier, defaults to the current module
    * `:restart` - when the child should be restarted, defaults to `:permanent`
    * `:shutdown` - how to shut down the child, either immediately or by giving
      it time to shut down

  For example:

      use Postgrex.Replication, restart: :transient, shutdown: 10_000

  See the "Child specification" section in the `Supervisor` module for more
  detailed information. The `@doc` annotation immediately preceding
  `use Postgrex.Replication` will be attached to the generated `child_spec/1`
  function.

  ## Name registration

  A `Postgrex.Replication` is bound to the same name registration rules as a
  `GenServer`. Read more about them in the `GenServer` docs.
  """

  use Connection
  require Logger
  import Bitwise

  alias Postgrex.Protocol

  @doc false
  defstruct protocol: nil,
            state: nil,
            auto_reconnect: false,
            reconnect_backoff: 500,
            repl_opts: nil,
            copy_opts: nil

  ## PUBLIC API ##

  @type server :: GenServer.server()
  @type state :: term
  @type copy :: binary
  @timeout 5000
  @max_lsn_component_size 8
  @max_uint64 18_446_744_073_709_551_615
  @max_copy_messages 500

  @doc """
  Callback for process initialization.

  This is called once and before the Postgrex connection is established.
  """
  @callback init(term) :: {:ok, state}

  @doc """
  Receives `binary` replication messages.

  The format of the messages are described [under the START_REPLICATION
  section in PostgreSQL docs](https://www.postgresql.org/docs/14/protocol-replication.html).

  Some messages require explicit acknowledgement, which can be done
  by returning a list of binaries according to the replication protocol.
  """
  @callback handle_data(binary, state) ::
              {:noreply, [copy], state} | {:query, String.t(), state}

  @doc """
  Receives copy messages.

  The format of the message is `Postgrex.Result.t()` with the row values
  formatted as `text`. A `{:copy,done, table_name}` message will be sent
  to signal copying has completed.
  """
  @callback handle_copy(Postgrex.Result.t() | {:copy_done, String.t()}, state) ::
              {:noreply, [copy], state} | {:query, String.t(), state}

  @doc """
  Callback for `Kernel.send/2`.
  """
  @callback handle_info(term, state) ::
              {:noreply, [copy], state} | {:query, String.t(), state}

  @doc """
  Callback for `call/3`.

  Replies must be sent with `reply/2`.
  """
  @callback handle_call(term, GenServer.from(), state) ::
              {:noreply, [copy], state} | {:query, String.t(), state}

  @doc """
  Handles result from a query command.

  If any callback returns `{:query, String.t(), state}`,
  then this callback will be immediatelly called with
  the result of the query.
  """
  @callback handle_result(%Postgrex.Result{}, state) ::
              {:noreply, [copy], state} | {:query, String.t(), state}

  @optional_callbacks handle_info: 2, handle_call: 3, handle_copy: 2, handle_result: 2

  @doc """
  Replies to the given client.

  Wrapper for `GenServer.reply/2`.
  """
  defdelegate reply(client, reply), to: GenServer

  @doc """
  Calls the given replication server.

  Wrapper for `GenServer.call/3`.
  """
  defdelegate call(server, message, timeout \\ 5000), to: GenServer

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour Postgrex.Replication

      unless Module.has_attribute?(__MODULE__, :doc) do
        @doc """
        Returns a specification to start this module under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(init_arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [init_arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a replication process with the given callback `module`.

  ## Options

  The options that this function accepts are the same as those
  accepted by `Postgrex.start_link/1`, except for `:idle_interval`.

  It also accepts extra options for connection management, documented below.
  Also note this function also automatically set `:replication` to `"database"`
  as part of the connection `:parameters` if none is set yet.

  ### Connection options

    * `:sync_connect` - controls if the connection should be established on boot
      or asynchronously right after boot. Defaults to `true`.

    * `:auto_reconnect` - automatically attempt to reconnect to the database
      in event of a disconnection. See the
      [note about async connect and auto-reconnects](#module-async-connect-and-auto-reconnects)
      above. Defaults to `false`, which means the process terminates.

    * `:reconnect_backoff` - time (in ms) between reconnection attempts when
      `:auto_reconnect` is enabled. Defaults to `500`.
  """
  @spec start_link(module(), term(), Keyword.t()) ::
          {:ok, pid} | {:error, Postgrex.Error.t() | term}
  def start_link(module, arg, opts) do
    {server_opts, opts} = Keyword.split(opts, [:name])
    opts = Keyword.put_new(opts, :sync_connect, true)
    connection_opts = Postgrex.Utils.default_opts(opts)
    Connection.start_link(__MODULE__, {module, arg, connection_opts}, server_opts)
  end

  @doc """
  Starts logical replication on a slot.

  Replication can be started on an already existing slot or a temporary
  slot can be created as part of this function. See the `:slot_name` and
  `:create_temporary_slot` options for more details. If the slot's plugin
  requires additional options, the `:plugin_opts` option must be specified.

  If the connection was started with `:auto_reconnect` set to `true`, then
  replication will automatically restart with the options passed into this
  function. Providing a temporary slot to the `:slot_name` option will cause
  reconnection to fail. This is due to the fact that temporary slots are
  dropped when a PostgreSQL session ends. If you wish to use `:auto_reconnect`
  with a temporary slot, use the `:create_temporary_slot` option instead. This
  provides the information needed to recreate the slot.

  If replication is sucessfully started, this function will return `{:ok, nil}` when
  `:slot_name` is provided or `{:ok, Postgrex.Result()}` when `:create_temporary_slot`
  is provided. `{:ok, Postgrex.Result()}` is the result from creating the temporary
  slot and is identical to what is returned by `create_slot/4`.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:slot_name` - It must be a string and is the name of an existing
      slot that will be used to start replication. Providing either this
      option or `:create_temporary_slot` is required. If neither of these
      options are provided, an error is raised.

    * `:create_temporary_slot` - It must be a keyword list and is used to
      create a temporary slot for replication. The `:slot_name`, `:plugin`
      and `:snapshot` keywords must be provided. See `create_slot/4` for
      more details about these keywords. Providing either this option or
      `:slot_name` is required. If neither of these options are provided,
      an error is raised.

    * `:plugin_opts` - It must be a keyword list and is used to configure
      the output plugin assigned to the slot.

    * `:start_pos` - The LSN value to start replication from. Must be
      formatted as a string of two hexadecimal numbers of up to 8 digits
      each, separated by a slash. e.g. `1/F73E0220`.
      Defaults to `0/0`.

    * `:max_messages` - The maximum number of replications messages that can be
      accumulated from the wire until they are relayed to `handle_data/2`.
      Defaults to `500`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of this command,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec start_replication(server, Keyword.t()) ::
          {:ok, Postgrex.Result.t() | nil} | {:error, Postgrex.Error.t() | :stream_in_progress}
  def start_replication(pid, opts \\ []) do
    opts =
      cond do
        Keyword.has_key?(opts, :slot_name) ->
          Keyword.drop(opts, [:create_temporary_slot])

        Keyword.has_key?(opts, :create_temporary_slot) ->
          {slot_opts, opts} = Keyword.pop(opts, :create_temporary_slot)
          slot_opts = Keyword.put(slot_opts, :temporary, true)

          if valid_slot_options?(slot_opts) do
            slot_opts ++ opts
          else
            raise ArgumentError,
                  "expected :slot_name, :plugin and :snapshot to be provided in :create_temporary_slot"
          end

        true ->
          raise ArgumentError, "expected one of :slot_name or :create_temporary slot"
      end

    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {__MODULE__, :start_replication, opts}, timeout)
  end

  @doc """
  Streams a copy of the given table.

  This function is meant to aid in the initial synchronization of large
  databases with high traffic. For simpler situations, it may be desirable
  to use a non-replication connection to copy the table.

  In addition to copying the table, a replication slot will be created using
  the `slot_name` and `plugin` arguments passed into this function. The slot
  is temporary by default but it can be changed via the `:temporary` option.
  The replication slot is returned to the user so it can be used to catch up
  with the changes that occur during copying. The slot's `:snapshot` value
  is automatically set to `:use` to ensure the user can start replication where
  the copying transaction left off.

  The ideal situation for using this function is to have a single main
  replication process and a pool of secondary replication processes, each
  one responsible for synchronizing a single table at a time. Synchronization
  is comprised of copying the table and then streaming the replication
  messages that were missed during that time. Once completed, the process
  can be terminated and control of the table's replication messages given back
  to the main process. The strategy limits the amount of WAL that must be retained
  and the amount of time transaction IDs must be held onto. These are important
  considerations to prevent disk bloat and transaction ID wraparound.

  If the connection was started with `:auto_reconnect` set to `true`, then
  copying will automatically restart from the beginning. You must ensure
  your system will not be affected by receiving duplicate messages.

  If copying is sucessfully started, `{:ok, Postgrex.Result()}` will be returned.
  `{:ok, Postgrex.Result()}` is the result from creating the replication slot
  and is identical to what is returned by `create_slot/4`.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:temporary` - When `true`, the slot will automatically drop when a session
      finishes. When `false`, the slot will persist outside of the session.
      Note that `false`  can lead to an unwanted build-up of WAL segments
      that eventually kill your primary instance. Prior to PostgreSQL 13, replication
      slots stop WAL segments from being removed until they are read by a consumer.
      Since PostgreSQL 13, the system parameter `max_slot_wal_keep_size` can be used
      to prevent this.
      [See PostgreSQL docs](https://www.postgresql.org/docs/current/runtime-config-replication.html).
      Defaults to `true`.

    * `:max_messages` - The maximum number of messages that can be
      accumulated from the wire until they are relayed to `handle_data/2`.
      One message corresponds to one data row.
      Defaults to `500`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.
  """
  @spec copy_table(server, String.t(), String.t(), atom(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()} | {:error, Postgrex.Error.t() | :stream_in_progress}
  def copy_table(pid, table_name, slot_name, plugin, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    opts = opts |> Keyword.put(:snapshot, :use) |> Keyword.put_new(:temporary, true)
    opts = [table_name: table_name, slot_name: slot_name, plugin: plugin] ++ opts
    call(pid, {__MODULE__, :copy_table, opts}, timeout)
  end

  @doc """
  Returns the string representation of an LSN value, given its integer representation.

  It returns `:error` if the provided integer falls outside the range for a valid
  unsigned 64-bit integer.

  ## Log Sequence Numbers

  PostgreSQL uses two representations for the Log Sequence Number (LSN):

    * An unsigned 64-bit integer. Used internally by PostgreSQL and sent in the XLogData
    replication messages.

    * A string of two hexadecimal numbers of up to eight digits each, separated
    by a slash. e.g. `1/F73E0220`. This is the form accepted by `start_replication/2`.

  For more information on Log Sequence Numbers, see
  [PostgreSQL pg_lsn docs](https://www.postgresql.org/docs/current/datatype-pg-lsn.html) and
  [PostgreSQL WAL internals docs](https://www.postgresql.org/docs/current/wal-internals.html).
  """
  @spec encode_lsn(integer) :: {:ok, String.t()} | :error
  def encode_lsn(lsn) when is_integer(lsn) do
    if 0 <= lsn and lsn <= @max_uint64 do
      <<file_id::32, offset::32>> = <<lsn::64>>
      {:ok, Integer.to_string(file_id, 16) <> "/" <> Integer.to_string(offset, 16)}
    else
      :error
    end
  end

  @doc """
  Returns the integer representation of an LSN value, given its string representation.

  It returns `:error` if the provided string is not a valid LSN.

  ## Log Sequence Numbers

  PostgreSQL uses two representations for the Log Sequence Number (LSN):

    * An unsigned 64-bit integer. Used internally by PostgreSQL and sent in the XLogData
    replication messages.

    * A string of two hexadecimal numbers of up to eight digits each, separated
    by a slash. e.g. `1/F73E0220`. This is the form accepted by `start_replication/2`.

  For more information on Log Sequence Numbers, see
  [PostgreSQL pg_lsn docs](https://www.postgresql.org/docs/current/datatype-pg-lsn.html) and
  [PostgreSQL WAL internals docs](https://www.postgresql.org/docs/current/wal-internals.html).
  """
  @spec decode_lsn(String.t()) :: {:ok, integer} | :error
  def decode_lsn(lsn) when is_binary(lsn) do
    with [file_id, offset] <- String.split(lsn, "/", trim: true),
         true <- byte_size(file_id) <= @max_lsn_component_size,
         true <- byte_size(offset) <= @max_lsn_component_size,
         {file_id, ""} when file_id >= 0 <- Integer.parse(file_id, 16),
         {offset, ""} when offset >= 0 <- Integer.parse(offset, 16) do
      {:ok, file_id <<< 32 ||| offset}
    else
      _ -> :error
    end
  end

  ## CALLBACKS ##

  @doc false
  def init({mod, arg, opts}) do
    case mod.init(arg) do
      {:ok, mod_state} ->
        opts =
          Keyword.update(
            opts,
            :parameters,
            [replication: "database"],
            &Keyword.put_new(&1, :replication, "database")
          )

        {auto_reconnect, opts} = Keyword.pop(opts, :auto_reconnect, false)
        {reconnect_backoff, opts} = Keyword.pop(opts, :reconnect_backoff, 500)

        state = %__MODULE__{
          auto_reconnect: auto_reconnect,
          reconnect_backoff: reconnect_backoff,
          state: {mod, mod_state}
        }

        put_opts(opts)

        if opts[:sync_connect] do
          case connect(:init, state) do
            {:ok, _} = ok -> ok
            {:backoff, _, _} = backoff -> backoff
            {:stop, reason, _} -> {:stop, reason}
          end
        else
          {:connect, :init, state}
        end
    end
  end

  @doc false
  def connect(_, s) do
    case Protocol.connect(opts()) do
      {:ok, protocol} ->
        maybe_restart_streaming(%{s | protocol: protocol})

      {:error, reason} ->
        if s.auto_reconnect do
          {:backoff, s.reconnect_backoff, s}
        else
          {:stop, reason, s}
        end
    end
  end

  @doc false
  def handle_call(
        {__MODULE__, :start_replication, opts},
        _from,
        %{repl_opts: nil, copy_opts: nil} = s
      ) do
    statement = command(:start_replication, opts)

    with {:ok, slot, protocol} <- maybe_create_slot(opts, s.protocol),
         {:ok, protocol} <- Protocol.handle_replication(statement, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      {:reply, {:ok, slot}, %{s | protocol: protocol, repl_opts: opts}}
    else
      {:error, reason, protocol} ->
        {:reply, {:error, reason}, %{s | protocol: protocol}}

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)
    end
  end

  def handle_call({__MODULE__, :copy_table, opts}, _from, %{repl_opts: nil, copy_opts: nil} = s) do
    table_name = Keyword.fetch!(opts, :table_name)
    begin_statement = command(:begin_copy, opts)
    create_slot_statement = command(:create_slot, opts)
    copy_statement = command(:copy_table, opts)

    with {:ok, _, protocol} <- Protocol.handle_simple(begin_statement, s.protocol),
         {:ok, slot, protocol} <- Protocol.handle_simple(create_slot_statement, protocol),
         {:ok, columns, protocol} <- table_columns(table_name, protocol),
         {:ok, protocol} <- Protocol.handle_copy_table(copy_statement, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      s = %{s | protocol: protocol, copy_opts: Keyword.put(opts, :columns, columns)}
      {:reply, {:ok, slot}, s}
    else
      {:error, reason, protocol} ->
        {:reply, {:error, reason}, %{s | protocol: protocol}}

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)
    end
  end

  def handle_call({__MODULE__, _, _}, _from, s) do
    {:reply, {:error, :stream_in_progress}, s}
  end

  def handle_call(msg, from, %{state: {mod, mod_state}} = s) do
    handle(mod, :handle_call, [msg, from, mod_state], s, from)
  end

  @doc false
  def handle_info(msg, %{protocol: protocol} = s) do
    max_copies = max_copy_messages(s)

    case Protocol.handle_copy_recv(msg, max_copies, protocol) do
      {:ok, copies, protocol} ->
        copy_recv_dispatch(copies, %{s | protocol: protocol})

      :unknown ->
        %{state: {mod, mod_state}} = s

        if function_exported?(mod, :handle_info, 2) do
          handle(mod, :handle_info, [msg, mod_state], s, nil)
        else
          {:noreply, s}
        end

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, s)
    end
  end

  defp copy_recv_dispatch(copies, %{copy_opts: nil} = s), do: handle_data(copies, s)
  defp copy_recv_dispatch(copies, s), do: handle_copy(copies, s)

  defp handle_data([], s), do: {:noreply, s}

  defp handle_data([copy | copies], %{state: {mod, mod_state}} = s) do
    with {:noreply, s} <- handle(mod, :handle_data, [copy, mod_state], s, nil) do
      handle_data(copies, s)
    end
  end

  defp handle_copy([], s), do: {:noreply, s}

  defp handle_copy([:copy_done | copies], %{state: {mod, mod_state}} = s) do
    copy = copy_message(:copy_done, s.copy_opts, nil)

    with {:noreply, s} <- handle(mod, :handle_copy, [copy, mod_state], s, nil),
         {:ok, _, protocol} <- Protocol.handle_simple("COMMIT", s.protocol) do
      handle_copy(copies, %{s | protocol: protocol, copy_opts: nil})
    else
      {:error, reason, _protocol} ->
        # If we can't commit the copy or drop the slot, we assume that auto_reconnect
        # can't solve it either, so we just raise the error as a readable message.
        raise reason

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)

      reconnect_or_stop ->
        reconnect_or_stop
    end
  end

  defp handle_copy([copy | copies], %{state: {mod, mod_state}} = s) do
    copy = copy_message(copy, s.copy_opts, s.protocol)

    with {:noreply, s} <- handle(mod, :handle_copy, [copy, mod_state], s, nil) do
      handle_copy(copies, s)
    end
  end

  defp handle(mod, fun, args, s, from) do
    case apply(mod, fun, args) do
      {:noreply, [], mod_state} ->
        s = %{s | state: {mod, mod_state}}
        {:noreply, s}

      {:noreply, replies, mod_state} ->
        s = %{s | state: {mod, mod_state}}

        case Protocol.handle_copy_send(replies, s.protocol) do
          :ok -> {:noreply, s}
          {error, reason, protocol} -> reconnect_or_stop(error, reason, protocol, s)
        end

      {:query, query, mod_state} ->
        case s do
          %{repl_opts: nil, copy_opts: nil} ->
            case Protocol.handle_simple(query, [], s.protocol) do
              {:ok, %Postgrex.Result{} = result, protocol} ->
                handle(mod, :handle_result, [result, mod_state], %{s | protocol: protocol}, from)

              {error, reason, protocol} ->
                from && reply(from, {error, reason})
                reconnect_or_stop(error, reason, protocol, %{s | state: {mod, mod_state}})
            end

          _ ->
            reply(from, {:error, :stream_in_progress})
            raise "cannot perform queries after replication or copying has started"
        end
    end
  end

  defp reconnect_or_stop(error, reason, protocol, %{auto_reconnect: false} = s)
       when error in [:error, :disconnect] do
    {:stop, reason, %{s | protocol: protocol}}
  end

  defp reconnect_or_stop(error, _reason, _protocol, %{auto_reconnect: true} = s)
       when error in [:error, :disconnect] do
    {:connect, :reconnect, s}
  end

  defp maybe_restart_streaming(%{repl_opts: nil, copy_opts: nil} = s), do: {:ok, s}

  defp maybe_restart_streaming(%{copy_opts: nil} = s) do
    start = command(:start_replication, s.repl_opts)

    with {:ok, _slot, protocol} <- maybe_create_slot(s.repl_opts, s.protocol),
         {:ok, protocol} <- Protocol.handle_replication(start, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      {:ok, %{s | protocol: protocol}}
    else
      # If we can't restart replication, we assume that auto_reconnect can't
      # solve it either, so we just raise the error as a readable message.
      {_error, reason, _protocol} ->
        raise reason
    end
  end

  defp maybe_restart_streaming(%{repl_opts: nil} = s) do
    table_name = Keyword.fetch!(s.copy_opts, :table_name)
    begin_statement = command(:begin_copy, s.copy_opts)
    copy_statement = command(:copy_table, s.copy_opts)

    with {:ok, _, protocol} <- Protocol.handle_simple(begin_statement, s.protocol),
         {:ok, _, protocol} <- maybe_create_slot(s.copy_opts, protocol),
         {:ok, columns, protocol} <- table_columns(table_name, protocol),
         {:ok, protocol} <- Protocol.handle_copy_table(copy_statement, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      s = %{s | protocol: protocol, copy_opts: Keyword.put(s.copy_opts, :columns, columns)}
      {:ok, s}
    else
      # If we can't restart copying, we assume that auto_reconnect can't
      # solve it either, so we just raise the error as a readable message.
      {_error, reason, _protocol} ->
        raise reason
    end
  end

  defp maybe_create_slot(opts, protocol) do
    case Keyword.fetch(opts, :temporary) do
      {:ok, true} ->
        create_statement = command(:create_slot, opts)
        Protocol.handle_simple(create_statement, protocol)

      _ ->
        {:ok, nil, protocol}
    end
  end

  defp max_copy_messages(%{repl_opts: nil, copy_opts: nil}), do: nil

  defp max_copy_messages(%{repl_opts: nil} = s),
    do: Keyword.get(s.copy_opts, :max_messages, @max_copy_messages)

  defp max_copy_messages(s), do: Keyword.get(s.repl_opts, :max_messages, @max_copy_messages)

  defp copy_message(:copy_done, opts, _protocol) do
    table_name = Keyword.fetch!(opts, :table_name)
    {:copy_done, table_name}
  end

  defp copy_message(copy, opts, %{connection_id: connection_id}) do
    columns = Keyword.fetch!(opts, :columns)
    row = copy |> String.trim() |> String.split("\t")

    %Postgrex.Result{
      command: :copy_stream,
      num_rows: 1,
      rows: [row],
      columns: columns,
      connection_id: connection_id
    }
  end

  defp table_columns(table_name, protocol) do
    show_statement = command(:show, name: "server_version_num")

    with {:ok, show_result, protocol} <- Protocol.handle_simple(show_statement, protocol),
         %Postgrex.Result{rows: [[version]]} = show_result,
         columns_statement_opts = [server_version: version, table_name: table_name],
         columns_statement = command(:table_columns, columns_statement_opts),
         {:ok, columns_result, protocol} <- Protocol.handle_simple(columns_statement, protocol) do
      {:ok, Enum.flat_map(columns_result.rows, & &1), protocol}
    end
  end

  defp valid_slot_options?(opts) do
    with true <- Keyword.has_key?(opts, :slot_name),
         true <- Keyword.has_key?(opts, :plugin),
         true <- Keyword.has_key?(opts, :snapshot) do
      true
    end
  end

  ## Queries
  defp command(:create_slot, opts) do
    slot = Keyword.fetch!(opts, :slot_name)
    temporary? = Keyword.get(opts, :temporary, true)
    plugin = Keyword.fetch!(opts, :plugin)
    snapshot = Keyword.get(opts, :snapshot, :export)

    [
      "CREATE_REPLICATION_SLOT ",
      slot,
      (temporary? && " TEMPORARY LOGICAL ") || " LOGICAL ",
      Atom.to_string(plugin),
      snapshot(snapshot)
    ]
  end

  defp command(:start_replication, opts) do
    slot = Keyword.fetch!(opts, :slot_name)
    options = Keyword.get(opts, :plugin_opts, [])
    start_pos = Keyword.get(opts, :start_pos, "0/0")

    [
      "START_REPLICATION SLOT ",
      slot,
      " LOGICAL ",
      start_pos,
      escape_options(options)
    ]
  end

  defp command(:show, opts) do
    name = Keyword.fetch!(opts, :name)
    ["SHOW ", name]
  end

  defp command(:copy_table, opts) do
    table_name = Keyword.fetch!(opts, :table_name)
    ["COPY ", table_name, " TO STDOUT"]
  end

  defp command(:begin_copy, _opts), do: ["BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ"]

  defp command(:table_columns, opts) do
    attgenerated_version = 120_000
    table_name = Keyword.fetch!(opts, :table_name)
    version = Keyword.fetch!(opts, :server_version)

    [
      "SELECT attname FROM pg_catalog.pg_attribute ",
      "WHERE attnum > 0::pg_catalog.int2 ",
      "AND NOT attisdropped ",
      (String.to_integer(version) >= attgenerated_version && "AND attgenerated = ''") || "",
      "AND attrelid = ",
      escape_string(table_name),
      "::regclass ",
      "ORDER BY attnum"
    ]
  end

  defp snapshot(:noexport), do: " NOEXPORT_SNAPSHOT"
  defp snapshot(:export), do: " EXPORT_SNAPSHOT"
  defp snapshot(:use), do: " USE_SNAPSHOT"

  defp escape_options([]),
    do: ""

  defp escape_options(opts) do
    parts =
      Enum.map_intersperse(opts, ", ", fn {k, v} -> [Atom.to_string(k), ?\s, escape_string(v)] end)

    [?\s, ?(, parts, ?)]
  end

  defp escape_string(value) do
    [?', :binary.replace(to_string(value), "'", "''", [:global]), ?']
  end

  defp opts(), do: Process.get(__MODULE__)
  defp put_opts(opts), do: Process.put(__MODULE__, opts)
end
