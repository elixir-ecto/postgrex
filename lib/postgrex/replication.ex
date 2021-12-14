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
          {:ok, %{}}
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
      Postgrex.Replication.create_slot(pid, "postgrex", :pgoutput)
      Postgrex.Replication.start_replication(pid, "postgrex",
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
  @copy_done :copy_done
  @slot_name_prefix "pg_"
  @slot_name_rand_bytes 18
  @public_commands [
    :create_slot,
    :drop_slot,
    :start_replication,
    :show,
    :identify_system,
    :timeline_history,
    :copy_table,
    :publication_tables
  ]

  @doc """
  Callback for process initialization.

  This is called once and before the Postgrex connection is established.
  """
  @callback init(term) :: {:ok, state}

  @doc """
  Receives `binary` messages during replication and `Postgrex.Result.t()` messages during table copying.

  The format of the row values in the copy messages are `text`.

  The format of the replication messages are described [under the START_REPLICATION
  section in PostgreSQL docs](https://www.postgresql.org/docs/14/protocol-replication.html).

  Some replication messages require explicit acknowledgement, which can be done
  by returning a list of binaries according to the replication protocol.
  """
  @callback handle_data(binary, state) :: {:noreply, [copy], state}

  @doc """
  Callback for `Kernel.send/2`.
  """
  @callback handle_info(term, state) :: {:noreply, [copy], state}

  @doc """
  Callback for `call/3`.

  Replies must be sent with `reply/2`.
  """
  @callback handle_call(term, GenServer.from(), state) :: {:noreply, [copy], state}

  @optional_callbacks handle_info: 2, handle_call: 3

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
      `auto_reconnect` is enabled. Defaults to `500`.
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
  Creates a logical replication slot with the given name and output plugin.

  By default, PostgreSQL includes the `pgoutput` plugin.

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

    * `:snapshot` - The type of logical snapshot for the slot. Must be one of
      `:export`, `:noexport`, or `:use`.
      Defaults to `:export`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of this command,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec create_slot(server, String.t(), atom(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def create_slot(pid, slot_name, plugin, opts \\ []) do
    opts = [slot_name: slot_name, plugin: plugin] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:create_slot, opts}, timeout)
  end

  @doc """
  Drops logical replication slot with the given name.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:wait` - When `true`, blocks while the slot is being used by a connection.
      When `false`, returns an error if the slot is being used by a connection.
      Defaults to `false`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of this command,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec drop_slot(server, String.t(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def drop_slot(pid, slot_name, opts \\ []) do
    opts = [slot_name: slot_name] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:drop_slot, opts}, timeout)
  end

  @doc """
  Starts logical replication on the given slot. If the slot's plugin requires
  additional options, make sure to specify them using the `plugin_opts` option.

  If the connection was started with `auto_reconnect` set to `true`, then
  replication will automatically restart with the options passed into this
  function. You must ensure your system will not be affected by receiving
  duplicate WAL updates. Note that temporary slots cannot be restarted due to the
  fact that they automatically drop upon disconnect.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:plugin_opts` - It must be a keyword list and is used to configure
      the output plugin assigned to the given slot.

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
  @spec start_replication(server, String.t(), Keyword.t()) ::
          :ok | {:error, Postgrex.Error.t()} | {:error, :stream_in_progress}
  def start_replication(pid, slot_name, opts \\ []) do
    opts = [slot: slot_name] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:start_replication, opts}, timeout)
  end

  @doc """
  Returns the current setting of a run-time parameter.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of this command,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec show(server, String.t(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def show(pid, name, opts \\ []) when is_binary(name) do
    opts = [name: name] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:show, opts}, timeout)
  end

  @doc """
  Returns identification information for the server.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of this command,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec identify_system(server, Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def identify_system(pid, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:identify_system, opts}, timeout)
  end

  @doc """
  Returns the timeline history file for the specified timeline ID.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of this command,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec timeline_history(server, String.t(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def timeline_history(pid, timeline_id, opts \\ []) when is_binary(timeline_id) do
    opts = [timeline_id: timeline_id] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:timeline_history, opts}, timeout)
  end

  @doc """
  Returns the tables contained by the given publication using the system catalog
  [pg_publication_tables](https://www.postgresql.org/docs/current/view-pg-publication-tables.html).

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:timeout` - Call timeout.
      Defaults to `5000`.
  """
  @spec publication_tables(server, String.t(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def publication_tables(pid, publication_name, opts \\ []) when is_binary(publication_name) do
    opts = [publication_name: publication_name] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:publication_tables, opts}, timeout)
  end

  @doc """
  Starts streaming a copy of the given table.

  This function can be used to initially synchronize the target system with
  the primary PostgreSQL instance before starting replication. For increased
  performance, a pool of replication processes can be created, each one
  synchronizing a single table at at time.

  The function performs the following steps:

    * A transaction will begin with the `REPEATABLE READ` isolation level.

    * A replication slot will be created with option `:snap_shot = :use`.
      This ensures the data read during the transaction is consistent with
      the way it  was when the slot was created.

    * A `COPY` command will be issued for the given table and the rows
      will be streamed to `handle_data/2`. The messages will be of the type
      `Postgrex.Result.t()` with the row values having `text` format.

    * Once all the rows have been copied, a final message of the form
      `{:copy_done, table_name}` will be sent, the transaction will be
      committed and the slot (optionally) dropped.

  This function will return the created slot's information, the same as
  `create_slot/4`. This information may be useful, for example, with the
  following tasks:

    * Using the slot's `consistent_point` to determine where to start
      replication.

    * Using the slot to catch up with the changes that occurred during copying.
      In this scenario, there may be a single main replication process alongside
      secondary replication processes for each table. An individual secondary process
      can be used to copy the initial snapshot for a given table and then stream
      replication messages for that table until it is caught up to the main process.
      Once caught up, the secondary process can be terminated and control of the table
      given back to the main process. This strategy minimizes the amount of time a
      transaction must hold onto old transaction IDs as well as the amount of time
      a slot must hold onto old WAL files.

  If the connection was started with `auto_reconnect` set to `true`, then
  copying will automatically restart from the beginning. You must ensure
  your system will not be affected by receiving duplicate messages.

  This function will return `{:error, :stream_in_progress}` while a table
  is being copied or after replication has begun.

  ## Options

    * `:drop_slot` - Automatically drops the slot created by this function.
      If `false`, care must be taken by the caller to drop the slot once
      it is not needed anymore. See `create_slot/4` for more details.
      Defaults to `true`.

    * `:slot_opts` - Keyword list of options to create the slot with.
      Only the keys `:slot_name`, `:temporary` and `:plugin` are allowed.
      By default, a temporary slot with the `:pgoutput` plugin is created.
      See `create_slot/4` for more details.

    * `:max_messages` - The maximum number of messages that can be
      accumulated from the wire until they are relayed to `handle_data/2`.
      One message corresponds to one data row.
      Defaults to `500`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.
  """
  @spec copy_table(server, String.t(), Keyword.t()) ::
          {:ok, Postgrex.Result.t()}
          | {:error, Postgrex.Error.t()}
          | {:error, :stream_in_progress}
  def copy_table(pid, table_name, opts \\ []) when is_binary(table_name) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    {slot_opts, opts} = Keyword.pop(opts, :slot_opts, [])

    slot_opts =
      slot_opts
      |> Keyword.take([:slot_name, :temporary, :plugin])
      |> Keyword.put_new(:plugin, :pgoutput)
      |> Keyword.put_new(:slot_name, copy_table_slot_name())
      |> Keyword.put(:snapshot, :use)

    opts = [table_name: table_name] ++ slot_opts ++ opts

    call(pid, {:copy_table, opts}, timeout)
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
    by a slash. e.g. `1/F73E0220`. This is the form accepted by `start_replication/3`.

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
    by a slash. e.g. `1/F73E0220`. This is the form accepted by `start_replication/3`.

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
  def handle_call({:start_replication, opts}, _from, %{repl_opts: nil, copy_opts: nil} = s) do
    statement = command(:start_replication, opts)

    with {:ok, protocol} <- Protocol.handle_replication(statement, s.protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      s = %{s | protocol: protocol, repl_opts: opts}
      {:reply, :ok, s}
    else
      {:error, reason, protocol} ->
        {:reply, {:error, reason}, %{s | protocol: protocol}}

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)
    end
  end

  @doc false
  def handle_call({:copy_table, opts}, _from, %{repl_opts: nil, copy_opts: nil} = s) do
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

  @doc false
  def handle_call({name, opts}, _from, %{repl_opts: nil, copy_opts: nil} = s)
      when name in @public_commands do
    statement = command(name, opts)

    case Protocol.handle_simple(statement, s.protocol) do
      {:ok, %Postgrex.Result{} = result, protocol} ->
        {:reply, {:ok, result}, %{s | protocol: protocol}}

      {:error, reason, protocol} ->
        {:reply, {:error, reason}, %{s | protocol: protocol}}

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)
    end
  end

  @doc false
  def handle_call({name, _opts}, _from, s) when name in @public_commands,
    do: {:reply, {:error, :stream_in_progress}, s}

  @doc false
  def handle_call(msg, from, %{state: {mod, mod_state}} = s) do
    handle(mod, :handle_call, [msg, from, mod_state], s)
  end

  @doc false
  def handle_info(msg, %{protocol: protocol} = s) do
    max_copies = max_copy_messages(s)

    case Protocol.handle_copy_recv(msg, max_copies, protocol) do
      {:ok, copies, protocol} ->
        handle_data(copies, %{s | protocol: protocol})

      :unknown ->
        %{state: {mod, mod_state}} = s

        if function_exported?(mod, :handle_info, 2) do
          handle(mod, :handle_info, [msg, mod_state], s)
        else
          {:noreply, s}
        end

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, s)
    end
  end

  defp handle_data([], s), do: {:noreply, s}

  defp handle_data([copy | copies], %{copy_opts: nil} = s) do
    %{state: {mod, mod_state}} = s

    with {:noreply, s} <- handle(mod, :handle_data, [copy, mod_state], s) do
      handle_data(copies, s)
    end
  end

  defp handle_data([@copy_done | copies], s) do
    %{state: {mod, mod_state}, copy_opts: copy_opts} = s
    copy = format_text_copy(@copy_done, copy_opts, nil)

    with {:noreply, s} <- handle(mod, :handle_data, [copy, mod_state], s),
         {:ok, _, protocol} <- Protocol.handle_simple("COMMIT", s.protocol),
         {:ok, _, protocol} <- maybe_drop_slot(copy_opts, protocol) do
      handle_data(copies, %{s | protocol: protocol, copy_opts: nil})
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

  defp handle_data([copy | copies], s) do
    %{state: {mod, mod_state}, copy_opts: copy_opts, protocol: protocol} = s
    copy = format_text_copy(copy, copy_opts, protocol)

    with {:noreply, s} <- handle(mod, :handle_data, [copy, mod_state], s) do
      handle_data(copies, s)
    end
  end

  defp handle(mod, fun, args, s) do
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

    with {:ok, protocol} <- Protocol.handle_replication(start, s.protocol),
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

  defp copy_table_slot_name do
    rand_name =
      @slot_name_rand_bytes
      |> :crypto.strong_rand_bytes()
      |> Base.encode32(padding: false)

    "#{@slot_name_prefix}#{rand_name}"
  end

  defp max_copy_messages(%{repl_opts: nil, copy_opts: nil}), do: nil

  defp max_copy_messages(%{repl_opts: nil} = s),
    do: Keyword.get(s.copy_opts, :max_messages, @max_copy_messages)

  defp max_copy_messages(s), do: Keyword.get(s.repl_opts, :max_messages, @max_copy_messages)

  defp format_text_copy(@copy_done, opts, _) do
    table_name = Keyword.fetch!(opts, :table_name)
    {:copy_done, table_name}
  end

  defp format_text_copy(copy, opts, %{connection_id: connection_id}) do
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
         %Postgrex.Result{rows: [[version]]} <- show_result,
         columns_statement_opts <- [server_version: version, table_name: table_name],
         columns_statement <- command(:table_columns, columns_statement_opts),
         {:ok, columns_result, protocol} <- Protocol.handle_simple(columns_statement, protocol) do
      {:ok, Enum.flat_map(columns_result.rows, & &1), protocol}
    end
  end

  defp maybe_drop_slot(opts, protocol) do
    if Keyword.get(opts, :drop_slot, true) do
      statement = command(:drop_slot, opts)
      Protocol.handle_simple(statement, protocol)
    else
      {:ok, nil, protocol}
    end
  end

  defp maybe_create_slot(opts, protocol) do
    state_statement = command(:slot_state, slot_name: opts.slot_name)

    with {:ok, state_result, protocol} <- Protocol.handle_simple(state_statement, protocol) do
      if state_result.rows == [[]] do
        create_statement = command(:create_slot, opts)
        Protocol.handle_simple(create_statement, protocol)
      else
        {:ok, nil, protocol}
      end
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

  defp command(:drop_slot, opts) do
    slot = Keyword.fetch!(opts, :slot_name)
    wait? = Keyword.get(opts, :wait, false)

    [
      "DROP_REPLICATION_SLOT ",
      slot,
      (wait? && " WAIT") || ""
    ]
  end

  defp command(:start_replication, opts) do
    slot = Keyword.fetch!(opts, :slot)
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

  defp command(:identify_system, _opts), do: ["IDENTIFY_SYSTEM"]

  defp command(:timeline_history, opts) do
    timeline_id = Keyword.fetch!(opts, :timeline_id)
    ["TIMELINE_HISTORY ", timeline_id]
  end

  defp command(:copy_table, opts) do
    table_name = Keyword.fetch!(opts, :table_name)
    ["COPY ", table_name, " TO STDOUT"]
  end

  defp command(:begin_copy, _opts), do: ["BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ"]

  defp command(:publication_tables, opts) do
    publication_name = Keyword.fetch!(opts, :publication_name)
    ["SELECT * FROM pg_publication_tables WHERE pubname = ", escape_string(publication_name)]
  end

  defp command(:slot_state, opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    ["SELECT * FROM pg_replication_slots WHERE slot_name = ", escape_string(slot_name)]
  end

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
