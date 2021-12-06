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

  alias Postgrex.Protocol

  @doc false
  defstruct protocol: nil,
            state: nil,
            auto_reconnect: false,
            reconnect_backoff: 500,
            slot_opts: nil,
            replication_started: false,
            replication_opts: nil

  ## PUBLIC API ##

  @type server :: GenServer.server()
  @type state :: term
  @type copy :: binary
  @timeout 5000
  @temporary true
  @commands [:create_slot, :drop_slot, :start_replication]

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

  Once replication has begun, no other commands can be given and this
  function will return `{:error, :replication_started}`.

  ## Options

    * `:temporary` - When `true`, the slot will automatically drop when a session
      finishes. When `false`, the slot will persist outside of the session.
      Note that `false`  can lead to an unwanted build-up of WAL segments
      that eventually kill your primary instance. Prior to PostgreSQL 13, replication
      slots stop WAL segments from being removed until they are read by a consumer.
      Since PostgreSQL 13, the system parameter `max_slot_wal_keep_size` can be used
      to prevent this. [See PostgreSQL docs](https://www.postgresql.org/docs/current/runtime-config-replication.html)
      Defaults to `true`.

    * `:snapshot` - The type of logical snapshot for the slot. Must be one of
      `:export`, `:noexport`, or `:use`.
      Defaults to `:export`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of those options,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec create_slot(server, String.t(), atom(), Keyword.t()) ::
          :ok | {:error, Postgrex.Error.t()} | {:error, :replication_started}
  def create_slot(pid, slot_name, plugin, opts \\ []) do
    opts = [slot: slot_name, plugin: plugin] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:create_slot, opts}, timeout)
  end

  @doc """
  Drops logical replication slot with the given name.

  Once replication has begun, no other commands can be given and this
  function will return `{:error, :replication_started}`.

  ## Options

    * `:wait` - When `true`, blocks while the slot is being used by a connection.
      When `false`, returns an error if the slot is being used by a connection.
      Defaults to `false`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of those options,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec drop_slot(server, String.t(), Keyword.t()) ::
          :ok | {:error, Postgrex.Error.t()} | {:error, :replication_started}
  def drop_slot(pid, slot_name, opts \\ []) do
    opts = [slot: slot_name] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:drop_slot, opts}, timeout)
  end

  @doc """
  Starts logical replication on the given slot. If the slot's plugin requires
  additional options, make sure to specify them using the `plugin_opts` option.

  If the connection was started with `auto_reconnect` set to `true`, then
  replication will automatically be restarted with the replication options passed
  into this function. You must ensure your system will not be affected by receiving
  duplicate WAL updates.

  Once replication has begun, no other commands can be given and this
  function will return `{:error, :replication_started}`.

  ## Options

    * `:plugin_opts` - It must be a keyword list and is used to configure
      the output plugin assigned to the given slot.

    * `:start_pos` - The LSN value to start replication from. Must be
      formatted as a string of two hexadecimal numbers of up to 8 digits
      each, separated by a slash. e.g. `1/F73E0220`.
      Defaults to `0/0`.

    * `:timeout` - Call timeout.
      Defaults to `5000`.

  To better understand the meaning of those options,
  [see PostgreSQL replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  """
  @spec start_replication(server, String.t(), Keyword.t()) ::
          :ok | {:error, Postgrex.Error.t()} | {:error, :replication_started}
  def start_replication(pid, slot_name, opts \\ []) do
    opts = [slot: slot_name] ++ opts
    {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
    call(pid, {:start_replication, opts}, timeout)
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
        maybe_restart_replication(%{s | protocol: protocol})

      {:error, reason} ->
        if s.auto_reconnect do
          {:backoff, s.reconnect_backoff, s}
        else
          {:stop, reason, s}
        end
    end
  end

  @doc false
  def handle_call({name, _opts}, _from, %{replication_started: true} = s)
      when name in @commands,
      do: {:reply, {:error, :replication_started}, s}

  @doc false
  def handle_call({:start_replication = name, opts}, _from, s) do
    %{protocol: protocol} = s
    statement = command(:start_replication, opts)

    with {:ok, protocol} <- Protocol.handle_replication(statement, protocol),
         {:ok, protocol} <- Protocol.checkin(protocol) do
      {:reply, :ok, update_success_state(s, name, protocol, opts)}
    else
      {:error, reason, protocol} ->
        {:reply, {:error, reason}, %{s | protocol: protocol}}

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)
    end
  end

  @doc false
  def handle_call({name, opts}, _from, s) when name in @commands do
    %{protocol: protocol} = s
    statement = command(name, opts)

    case Protocol.handle_simple(statement, protocol) do
      {:ok, %Postgrex.Result{}, protocol} ->
        {:reply, :ok, update_success_state(s, name, protocol, opts)}

      {:error, reason, protocol} ->
        {:reply, {:error, reason}, %{s | protocol: protocol}}

      {:disconnect, reason, protocol} ->
        reconnect_or_stop(:disconnect, reason, protocol, s)
    end
  end

  @doc false
  def handle_call(msg, from, %{state: {mod, mod_state}} = s) do
    handle(mod, :handle_call, [msg, from, mod_state], s)
  end

  @doc false
  def handle_info(msg, %{protocol: protocol} = s) do
    case Protocol.handle_copy_recv(msg, protocol) do
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

  defp handle_data([copy | copies], %{state: {mod, mod_state}} = s) do
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

  defp maybe_restart_replication(%{replication_started: true} = s) do
    temporary? = Keyword.get(s.slot_opts, :temporary, @temporary)
    restart_replication(s, temporary?)
  end

  defp maybe_restart_replication(s), do: {:ok, s}

  defp restart_replication(s, true = _temporary?) do
    create = command(:create_slot, s.slot_opts)
    start = command(:start_replication, s.replication_opts)

    with {:ok, %Postgrex.Result{}, protocol} <- Protocol.handle_simple(create, s.protocol),
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

  defp restart_replication(s, false = _temporary?) do
    start = command(:start_replication, s.replication_opts)

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

  defp update_success_state(s, :create_slot, protocol, opts),
    do: %{s | protocol: protocol, slot_opts: opts}

  defp update_success_state(s, :start_replication, protocol, opts),
    do: %{s | protocol: protocol, replication_started: true, replication_opts: opts}

  defp update_success_state(s, _command, protocol, _opts),
    do: %{s | protocol: protocol}

  ## Queries
  defp(command(:create_slot, opts)) do
    slot = Keyword.fetch!(opts, :slot)
    temporary? = Keyword.get(opts, :temporary, @temporary)
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
    slot = Keyword.fetch!(opts, :slot)
    wait? = Keyword.get(opts, :wait, true)

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
