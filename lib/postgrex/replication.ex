defmodule Postgrex.Replication do
  @moduledoc ~S"""
  A process that receives and sends PostgreSQL replication messages.

  > Note: this module is still experimental and provides limited
  > functionality. In particular, it only currently handles temporary
  > logical replication with a WAL starting point specified by the
  > server. We are glad to discuss and receive pull requests that
  > extends the scope of the module.

  ## Logical replication

  Let's see how to use this module for connecting to PostgreSQL
  for logical replication. First of all, you need to configure the
  wal level in PostgreSQL to logical. Run this inside your PostgreSQL
  shell/configuration:

      ALTER SYSTEM SET wal_level='logical';
      ALTER SYSTEM SET max_wal_senders='10';
      ALTER SYSTEM SET max_replication_slots='10';

  Then **you must restart your server**.

  Then you must create a publication that we will replicate.
  This can be done in any session:

      CREATE PUBLICATION example FOR ALL TABLES;

  Now we are ready to create module

  Here is a simple example that listens to replication messages
  and prints them to the terminal. The message is written in the
  exact format given by the `pgoutput` streaming replication plugin.

      Mix.install([:postgrex])

      defmodule Example do
        use Postgrex.Replication

        def start_link(opts) do
          # Setup temporary logical replication with the pgoutput plugin.
          # proto_version must be 1 in recent pgoutput versions and
          # the publication name is also required.
          #
          # Finally, also automatically reconnect if we lose connection.
          extra_opts = [
            slot: "postgrex",
            logical: {:pgoutput, proto_version: 1, publication_names: "example"},
            temporary: true,
            snapshot: :noexport,
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

      {:ok, _pid} =
        Example.start_link(
          host: "localhost",
          database: "demo_dev",
          username: "postgres",
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
            reconnect_backoff: 500

  ## PUBLIC API ##

  @type server :: GenServer.server()
  @type state :: term
  @type copy :: binary

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

  It also accepts extra options for connection and replication
  management, documented in the sections below. Also note this
  function also automatically set `:replication` to `"database"`
  as part of the connection `:parameters` if none is set yet.

  ### Replication options

    * `:logical` - required. It must be a tuple `{plugin, options}`
      to configure logical replication with the given plugin and
      options. By default, PostgreSQL includes the `pgoutput` plugin.

    * `:slot` - required. The name of the replication slot.

    * `:temporary` - required and must be set to `true`.

    * `:snapshot` - optional. The type of logical snapshot for the
      slot. Must be one of `:export`, `:noexport`, or `:use`.
      Defaults to `:export`.

  To understanding the meaning of those options, [see PostgreSQL
  replication docs](https://www.postgresql.org/docs/14/protocol-replication.html).

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
    opts = opts()

    case Protocol.connect([types: nil] ++ opts()) do
      {:ok, protocol} ->
        s = %{s | protocol: protocol}
        {create, start} = commands(opts)

        with {:ok, %Postgrex.Result{}, protocol} <- Protocol.handle_simple(create, protocol),
             {:ok, protocol} <- Protocol.handle_replication(start, protocol),
             {:ok, protocol} <- Protocol.checkin(protocol) do
          {:ok, %{s | protocol: protocol}}
        else
          # If we can't start replication, we assume that auto_reconnect can't
          # solve it either, so we just raise the error as a readable message.
          {_error, reason, _protocol} ->
            raise reason
        end

      {:error, reason} ->
        if s.auto_reconnect do
          {:backoff, s.reconnect_backoff, s}
        else
          {:stop, reason, s}
        end
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

  ## Queries

  defp commands(opts) do
    slot = Keyword.fetch!(opts, :slot)
    true = Keyword.fetch!(opts, :temporary)
    {plugin, options} = Keyword.fetch!(opts, :logical)
    snapshot = Keyword.get(opts, :snapshot, :export)

    create = [
      "CREATE_REPLICATION_SLOT ",
      slot,
      " TEMPORARY LOGICAL ",
      Atom.to_string(plugin),
      snapshot(snapshot)
    ]

    start = [
      "START_REPLICATION SLOT ",
      slot,
      " LOGICAL 0/0",
      escape_options(options)
    ]

    {create, start}
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
