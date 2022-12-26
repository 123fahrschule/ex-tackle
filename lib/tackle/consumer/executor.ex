defmodule Tackle.Consumer.Executor do
  require Logger

  # let GenServer 1 sec for cleanup work
  use GenServer, shutdown: 1_000

  def republish_dead_messages(name_or_pid, how_many) do
    GenServer.cast(name_or_pid, {:republish_dead_messages, how_many})
  end

  def start_link(name, handler_module, overrides) do
    state = Enum.into(overrides, %{handler: handler_module})

    GenServer.start_link(__MODULE__, state, name: name)
  end

  def init(options) do
    state = connect(options)

    # so, we can cleanup with terminate callback
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  defp connect(options) do
    options
    |> Tackle.Consumer.State.configure!()
    |> setup(options)
  end

  defp setup(%{topology: topology} = state, options) do
    with {:ok, channel} <-
           setup_connection_channel(
             state.connection_id,
             state.rabbitmq_url,
             state.prefetch_count
           ) do
      Tackle.Consumer.Topology.setup!(channel, topology)

      # start actual consuming
      {:ok, _consumer_tag} = AMQP.Basic.consume(channel, topology.queue)

      # Sleep is needed for the ConnectionErrorsTest
      Process.sleep(2)

      Tackle.Consumer.State.started(state, channel: channel)
    else
      _ ->
        Process.send_after(self(), {:connect, options}, 3_000)
        %{}
    end
  end

  defp setup_connection_channel(connection_id, url, prefetch_count) do
    with {:ok, connection} <- Tackle.Connection.open(connection_id, url) do
      # Get notifications when the connection goes down
      Process.monitor(connection.pid)
      channel = Tackle.Channel.create(connection, prefetch_count)
      {:ok, channel}
    end
  end

  # Close channel on exit
  def terminate(_reason, %{channel: channel} = state) do
    state.channel |> Tackle.Channel.close()
  end
  def terminate(_reason, state), do: :ok

  def handle_info({:connect, options}, _state) do
    state = connect(options)

    {:noreply, state}
  end

  def handle_info({:basic_consume_ok, _}, state), do: {:noreply, state}
  def handle_info({:basic_cancel, _}, state), do: {:stop, :normal, state}
  def handle_info({:basic_cancel_ok, _}, state), do: {:stop, :normal, state}

  def handle_info({:basic_deliver, payload, %{delivery_tag: tag} = message_metadata}, state) do
    # try/rescue/catch stattdessen benutzen
    consume_callback = fn ->
      state.handler.handle_message(payload)
      :ok = AMQP.Basic.ack(state.channel, tag)
    end

    error_callback = fn reason ->
      Logger.error("Consumption failed: #{inspect(reason)}; payload: #{inspect(payload)}")
      retry(state, payload, message_metadata, reason)
      :ok = AMQP.Basic.nack(state.channel, tag, multiple: false, requeue: false)
    end

    spawn(fn -> delivery_handler(consume_callback, error_callback) end)

    {:noreply, state}
  end

  # Called if a monitored connection dies.
  def handle_info(
        {:DOWN, _, :process, pid, reason},
        %Tackle.Consumer.State{
          channel: %AMQP.Channel{conn: %AMQP.Connection{pid: pid}},
          topology: topology
        } = state
      ) do
    Logger.warn(
      "Connection process went down (#{inspect(reason)}). Stopping Consumer for queue #{topology.queue}"
    )

    {:stop, {:connection_lost, reason}, state}
  end

  def delivery_handler(consume_callback, error_callback) do
    Process.flag(:trap_exit, true)

    me = self()
    safe_consumer = fn -> safe_consumer(me, consume_callback) end

    pid = spawn_link(safe_consumer)

    receive do
      :ok ->
        :ok

      {:retry, reason} ->
        error_callback.(reason)

      {:EXIT, ^pid, :normal} ->
        :ok

      {:EXIT, ^pid, :shutdown} ->
        :ok

      {:EXIT, ^pid, {:shutdown, _reason}} ->
        :ok

      {:EXIT, ^pid, reason} ->
        error_callback.(reason)
    end
  end

  defp safe_consumer(receiver, consume_callback) do
    # try not to die, so we do not get all the notifications
    result =
      try do
        consume_callback.()
        :ok
      catch
        # retry on exit and throw(:retry) or throw({:retry, reason})
        :throw, :retry ->
          {:retry, {:retry_requested, __STACKTRACE__}}

        :throw, {:retry, reason} ->
          {:retry, {reason, __STACKTRACE__}}
      end

    # send result back to the receiver, so we can retry if needed
    send(receiver, result)
  end

  defp retry(
         state,
         payload,
         %{headers: headers} = message_metadata,
         error_reason
       ) do
    retry_count = Tackle.DelayedRetry.retry_count_from_headers(headers)

    # FIXME: try/rescue/catch stattdessen benutzen
    Task.start(fn ->
      current_attempt = retry_count + 1
      max_number_of_attemts = state.topology.retry_limit + 1
      {may_be_erlang_error, stacktrace} = error_reason
      elixir_exception = Exception.normalize(:error, may_be_erlang_error, stacktrace)

      state.handler.on_error(
        payload,
        message_metadata,
        {elixir_exception, stacktrace},
        current_attempt,
        max_number_of_attemts
      )
    end)

    retry_message_options = [
      persistent: true,
      headers: [
        retry_count: retry_count + 1
      ]
    ]

    if retry_count < state.topology.retry_limit do
      Logger.debug("Sending message to a delay queue")

      Tackle.DelayedRetry.publish(
        state.rabbitmq_url,
        state.topology.delay_queue,
        payload,
        retry_message_options
      )
    else
      Logger.debug("Sending message to a dead messages queue")

      Tackle.DelayedRetry.publish(
        state.rabbitmq_url,
        state.topology.dead_queue,
        payload,
        retry_message_options
      )
    end
  end

  def handle_cast({:republish_dead_messages, how_many}, state) do
    Tackle.Republisher.republish(
      state.rabbitmq_url,
      state.topology.dead_queue,
      state.topology.message_exchange,
      state.topology.routing_key,
      how_many
    )

    {:noreply, state}
  end

  def handle_call({:option, option_name}, _from, state) do
    option_value = Map.get(state, option_name)
    {:reply, option_value, state}
  end
end
