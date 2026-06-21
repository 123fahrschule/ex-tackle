defmodule Tackle.Publisher do
  @moduledoc """
  A long-lived, supervised publisher: one process per connection name.

  Each publisher owns a persistent AMQP connection and a small pool of channels
  (`Tackle.Publisher.ChannelPool`). Publishing checks a channel out of the pool
  and runs in the calling process, so the hot path neither opens a connection
  nor a channel per message. Exchanges are declared only once per
  `{connection, exchange, type}`; the cache is invalidated and the exchanges are
  re-declared after a reconnect.

  Publisher confirms are optional (see `Tackle` configuration). When enabled,
  `publish/6` waits for the broker to confirm the message and returns an error
  tuple on a negative confirm, a timeout, or an unroutable message instead of
  silently dropping it.

  Publishers are started lazily on first publish via
  `Tackle.Publisher.Supervisor`, so the application boots without a broker.
  """

  use GenServer

  require Logger

  @registry Tackle.Publisher.Registry

  @default_pool_size 4
  @default_confirm_timeout 5_000
  @default_reconnect_interval 1_000

  # How long to wait for a channel to become available in the pool.
  @checkout_timeout 5_000
  # How long the first publish may block while the connection is established.
  @ready_timeout 15_000
  # Grace window to observe an asynchronous `basic.return` (unroutable message)
  # after a confirm, in milliseconds.
  @return_grace 50

  ## Public API

  @doc """
  Publish `payload` to `exchange` with `routing_key` through the pooled
  publisher registered under `name`, starting it if necessary.

  `opts` are AMQP message options (e.g. `persistent: true`, `headers: [...]`)
  plus the publisher options `:confirm`, `:confirm_timeout` and
  `:exchange_type`, which are consumed here and not forwarded to AMQP.

  Returns `:ok` or `{:error, reason}`.
  """
  def publish(name, rabbitmq_url, exchange, routing_key, payload, opts \\ []) do
    case resolve(name, rabbitmq_url) do
      {:ok, pid, meta} ->
        {confirm?, confirm_timeout, type, message_opts} = split_opts(opts, meta)

        with :ok <- ensure_declared(pid, meta, exchange, type) do
          checkout_and_publish(
            meta,
            exchange,
            routing_key,
            payload,
            message_opts,
            confirm?,
            confirm_timeout
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Whether `{exchange, type}` is currently cached as declared for `name`.

  Intended for tests; returns `false` when the publisher is not running.
  """
  def declared?(name, exchange, type) do
    case Registry.lookup(@registry, name) do
      [{_pid, %{declared: table}}] -> :ets.member(table, {exchange, type})
      _ -> false
    end
  end

  @doc false
  # Pure mapping from a confirm outcome to a publish result. Kept separate so it
  # can be unit-tested without a broker.
  def interpret_confirm(_confirm, true = _returned?), do: {:error, :unroutable}
  def interpret_confirm(true, false), do: :ok
  def interpret_confirm(false, false), do: {:error, :nack}
  def interpret_confirm(:timeout, false), do: {:error, :confirm_timeout}

  ## Child spec / start

  def child_spec({name, _rabbitmq_url} = arg) do
    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [arg]},
      restart: :transient,
      type: :worker
    }
  end

  def start_link({name, rabbitmq_url}) do
    GenServer.start_link(__MODULE__, {name, rabbitmq_url}, name: via(name))
  end

  ## GenServer callbacks

  @impl GenServer
  def init({name, rabbitmq_url}) do
    Process.flag(:trap_exit, true)

    declared = :ets.new(:tackle_publisher_declared, [:set, :public, read_concurrency: true])

    state = %{
      name: name,
      rabbitmq_url: rabbitmq_url,
      connection: nil,
      monitor_ref: nil,
      control_channel: nil,
      pool: nil,
      declared: declared,
      connected?: false,
      confirms?: confirms_default(),
      confirm_timeout: confirm_timeout_default(),
      pool_size: pool_size_default(),
      reconnect_interval: reconnect_interval_default()
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state) do
    {:noreply, connect(state)}
  end

  # Blocks the first publisher until the connection is up, then serves the
  # cached metadata. Subsequent publishes read the metadata straight from the
  # registry without touching this process.
  @impl GenServer
  def handle_call(:get_meta, _from, state) do
    state = if state.connected?, do: state, else: connect(state)

    if state.connected? do
      {:reply, {:ok, meta(state)}, state}
    else
      {:reply, {:error, :not_connected}, state}
    end
  end

  def handle_call({:declare, exchange, type}, _from, state) do
    state = if state.connected?, do: state, else: connect(state)

    cond do
      not state.connected? ->
        {:reply, {:error, :not_connected}, state}

      :ets.member(state.declared, {exchange, type}) ->
        {:reply, :ok, state}

      true ->
        try do
          Tackle.Exchange.create(state.control_channel, exchange, type)
          :ets.insert(state.declared, {{exchange, type}})
          {:reply, :ok, state}
        catch
          kind, reason ->
            Logger.error(
              "Tackle.Publisher(#{inspect(state.name)}) failed to declare exchange " <>
                "'#{exchange}': #{inspect({kind, reason})}"
            )

            {:reply, {:error, :declare_failed}, state}
        end
    end
  end

  # The owned connection went down: invalidate everything and reconnect.
  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{monitor_ref: ref} = state) do
    Logger.warning(
      "Tackle.Publisher(#{inspect(state.name)}) connection went down due to " <>
        "#{inspect(reason)}. Reconnecting."
    )

    {:noreply, disconnect_and_reschedule(state)}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(:reconnect, state) do
    {:noreply, connect(state)}
  end

  # Fetching OS certificates while opening a (secure) connection sends this on
  # darwin; ignore it (mirrors Tackle.Consumer.Executor).
  def handle_info({:EXIT, _port, :normal}, state), do: {:noreply, state}

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    stop_pool(state)
    close_control_channel(state)
    :ok
  end

  ## Internals — connection lifecycle

  defp connect(%{connected?: true} = state), do: state

  defp connect(state) do
    with {:ok, connection} <- Tackle.Connection.open(state.name, state.rabbitmq_url),
         {:ok, control_channel} <- AMQP.Channel.open(connection),
         {:ok, pool} <- start_pool(state, connection) do
      ref = Process.monitor(connection.pid)
      :ets.delete_all_objects(state.declared)

      state = %{
        state
        | connection: connection,
          monitor_ref: ref,
          control_channel: control_channel,
          pool: pool,
          connected?: true
      }

      # Publish readiness to the registry so callers can take the fast path.
      Registry.update_value(@registry, state.name, fn _ -> meta(state) end)

      Logger.info("Tackle.Publisher(#{inspect(state.name)}) connected")
      state
    else
      error ->
        Logger.error(
          "Tackle.Publisher(#{inspect(state.name)}) failed to connect: #{inspect(error)}"
        )

        schedule_reconnect(state)
        %{state | connected?: false}
    end
  end

  defp disconnect_and_reschedule(state) do
    stop_pool(state)
    :ets.delete_all_objects(state.declared)
    Registry.update_value(@registry, state.name, fn _ -> nil end)

    state = %{
      state
      | connection: nil,
        monitor_ref: nil,
        control_channel: nil,
        pool: nil,
        connected?: false
    }

    schedule_reconnect(state)
    state
  end

  defp schedule_reconnect(state) do
    Process.send_after(self(), :reconnect, state.reconnect_interval)
  end

  defp start_pool(state, connection) do
    NimblePool.start_link(
      worker: {Tackle.Publisher.ChannelPool, %{connection: connection}},
      pool_size: state.pool_size,
      name: pool_via(state.name)
    )
  end

  defp stop_pool(%{pool: nil}), do: :ok

  defp stop_pool(%{pool: pool}) do
    try do
      NimblePool.stop(pool, :shutdown, 5_000)
    catch
      _kind, _reason -> :ok
    end

    :ok
  end

  defp close_control_channel(%{control_channel: nil}), do: :ok

  defp close_control_channel(%{control_channel: channel}) do
    if Process.alive?(channel.pid) do
      try do
        AMQP.Channel.close(channel)
      catch
        _kind, _reason -> :ok
      end
    end

    :ok
  end

  ## Internals — publishing

  defp resolve(name, rabbitmq_url) do
    case Registry.lookup(@registry, name) do
      [{pid, %{} = meta}] ->
        {:ok, pid, meta}

      _ ->
        with {:ok, pid} <- Tackle.Publisher.Supervisor.ensure_started(name, rabbitmq_url),
             {:ok, meta} <- GenServer.call(pid, :get_meta, @ready_timeout) do
          {:ok, pid, meta}
        end
    end
  end

  # The default exchange ("") always exists and must not be (re)declared.
  defp ensure_declared(_pid, _meta, "", _type), do: :ok

  defp ensure_declared(pid, meta, exchange, type) do
    if :ets.member(meta.declared, {exchange, type}) do
      :ok
    else
      GenServer.call(pid, {:declare, exchange, type})
    end
  end

  defp checkout_and_publish(meta, exchange, routing_key, payload, message_opts, confirm?, timeout) do
    NimblePool.checkout!(
      meta.pool,
      :checkout,
      fn _from, channel ->
        {publish_on_channel(
           channel,
           exchange,
           routing_key,
           payload,
           message_opts,
           confirm?,
           timeout
         ), channel}
      end,
      @checkout_timeout
    )
  catch
    kind, reason ->
      {:error, {kind, reason}}
  end

  defp publish_on_channel(channel, exchange, routing_key, payload, message_opts, false, _timeout) do
    AMQP.Basic.publish(channel, exchange, routing_key, payload, message_opts)
  end

  defp publish_on_channel(channel, exchange, routing_key, payload, message_opts, true, timeout) do
    # The checkout function runs in the calling process, so the broker's
    # `basic.return` for an unroutable (mandatory) message is forwarded here by
    # the channel's consumer process. That forward is asynchronous and may land
    # just after `wait_for_confirms/2` returns, so we allow a short grace window
    # to observe it before deciding the message was routed.
    AMQP.Basic.return(channel, self())
    flush_returns()

    :ok =
      AMQP.Basic.publish(
        channel,
        exchange,
        routing_key,
        payload,
        Keyword.put(message_opts, :mandatory, true)
      )

    confirm = AMQP.Confirm.wait_for_confirms(channel, timeout)
    interpret_confirm(confirm, received_return?(@return_grace))
  end

  defp flush_returns do
    receive do
      {:basic_return, _payload, _meta} -> flush_returns()
    after
      0 -> :ok
    end
  end

  defp received_return?(grace) do
    receive do
      {:basic_return, _payload, _meta} -> true
    after
      grace -> false
    end
  end

  ## Internals — config & registry

  defp split_opts(opts, meta) do
    confirm? = Keyword.get(opts, :confirm, meta.confirms)
    confirm_timeout = Keyword.get(opts, :confirm_timeout, meta.confirm_timeout)
    type = Keyword.get(opts, :exchange_type, exchange_type_default())

    message_opts =
      opts
      |> Keyword.drop([:confirm, :confirm_timeout, :exchange_type])
      |> Keyword.put_new(:persistent, true)

    {confirm?, confirm_timeout, type, message_opts}
  end

  defp meta(state) do
    %{
      pool: pool_via(state.name),
      declared: state.declared,
      confirms: state.confirms?,
      confirm_timeout: state.confirm_timeout
    }
  end

  defp via(name), do: {:via, Registry, {@registry, name}}
  defp pool_via(name), do: {:via, Registry, {@registry, {:pool, name}}}

  defp confirms_default, do: Application.get_env(:tackle, :publisher_confirms, false)

  defp confirm_timeout_default,
    do: Application.get_env(:tackle, :publisher_confirm_timeout, @default_confirm_timeout)

  defp pool_size_default,
    do: Application.get_env(:tackle, :publisher_pool_size, @default_pool_size)

  defp reconnect_interval_default,
    do: Application.get_env(:tackle, :publisher_reconnect_interval, @default_reconnect_interval)

  defp exchange_type_default, do: Application.get_env(:tackle, :exchange_type, :direct)
end
