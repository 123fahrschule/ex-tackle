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
    # so, we can cleanup with terminate callback
    Process.flag(:trap_exit, true)

    state =
      options
      |> Tackle.Consumer.State.configure!()

    {:ok, state, {:continue, :try_setup}}
  end

  # Try to open channel and setup topology
  def handle_continue(:try_setup, %{channel: channel, consumer_tag: nil} = state)
      when not is_nil(channel) do
    {:noreply, state, {:continue, :try_consume}}
  end

  def handle_continue(:try_setup, %{channel: channel} = state) when not is_nil(channel) do
    {:noreply, state}
  end

  def handle_continue(:try_setup, %{channel_retry_ref: ref} = state) when is_reference(ref) do
    {:noreply, state}
  end

  def handle_continue(
        :try_setup,
        %{
          connection_id: connection_name,
          rabbitmq_url: rabbitmq_url,
          prefetch_count: prefetch_count,
          topology: topology,
          reconnect_interval: reconnect_interval,
          handler: handler_module
        } = state
      ) do
    with {:ok, connection} <- Tackle.Connection.open(connection_name, rabbitmq_url),
         {:ok, channel} <- Tackle.Channel.create(connection, prefetch_count) do
      # Get notifications when the connection goes down
      Process.monitor(channel.pid)
      Tackle.Consumer.Topology.setup!(channel, topology)

      {:noreply, %{state | channel: channel, channel_retry_ref: nil}, {:continue, :try_consume}}
    else
      {:error, _} = error ->
        Logger.error("#{handler_module} failed to setup channel due to `#{inspect(error)}`")
        ref = Process.send_after(self(), :retry_setup_after_delay, reconnect_interval)
        {:noreply, %{state | channel_retry_ref: ref}}
    end
  end

  # Try to start consumption
  def handle_continue(:try_consume, %{consume_retry_ref: nil} = state) do
    %{
      channel: channel,
      topology: topology,
      reconnect_interval: reconnect_interval,
      handler: handler_module
    } = state

    with {:ok, consumer_tag} <- AMQP.Basic.consume(channel, topology.queue) do
      {:noreply, %{state | consumer_tag: consumer_tag}}
    else
      {:error, _} = error ->
        Logger.error(
          "#{handler_module} failed to start consumption from `#{topology.queue}` due to `#{inspect(error)}`"
        )

        ref = Process.send_after(self(), :retry_consume_after_delay, reconnect_interval)
        {:noreply, %{state | consume_retry_ref: ref}}
    end
  end

  def handle_continue(:try_consume, state), do: {:noreply, state}

  # Retry opening channel and setup topology
  def handle_info(:retry_setup_after_delay, state = %{channel_retry_ref: nil}), do: {:noreply, state}

  def handle_info(:retry_setup_after_delay, state = %{channel_retry_ref: ref})
      when is_reference(ref) do
    {:noreply, %{state | channel_retry_ref: nil}, {:continue, :try_setup}}
  end

  # Retry start consumption
  def handle_info(:retry_consume_after_delay, state = %{consume_retry_ref: nil}),
    do: {:noreply, state}

  def handle_info(:retry_consume_after_delay, state = %{consume_retry_ref: ref})
      when is_reference(ref) do
    {:noreply, %{state | consume_retry_ref: nil}, {:continue, :try_consume}}
  end

  # Consume messages
  def handle_info({:basic_consume_ok, _}, %{handler: handler_module, topology: topology} = state) do
    Logger.info("#{handler_module} started consuming messages from `#{topology.queue}`")
    {:noreply, state}
  end

  def handle_info({:basic_cancel, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:basic_cancel_ok, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:basic_deliver, payload, %{delivery_tag: tag} = message_metadata}, state) do
    # try/rescue/catch stattdessen benutzen
    consume_callback = fn ->
      state.handler.handle_message(payload)
      :ok = AMQP.Basic.ack(state.channel, tag)
    end

    error_callback = fn reason ->
      Logger.error("Consumption failed: `#{inspect(reason)}`; payload: `#{inspect(payload)}`")
      retry(state, payload, message_metadata, reason)
      :ok = AMQP.Basic.nack(state.channel, tag, multiple: false, requeue: false)
    end

    spawn(fn -> delivery_handler(consume_callback, error_callback) end)

    {:noreply, state}
  end

  # Called if a monitored connection dies.
  def handle_info(
        {:DOWN, _ref, :process, channel_pid, reason},
        %{
          channel: %AMQP.Channel{pid: channel_pid},
          topology: topology,
          handler: handler_module
        } = state
      ) do
    Logger.warning(
      "#{handler_module} channel process went down due to `#{inspect(reason)}`. Reconnecting to `#{topology.queue}`."
    )

    state = %{state | channel: nil, channel_retry_ref: nil, consumer_tag: nil}
    {:noreply, state, {:continue, :try_setup}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  # This message is received because of fetching OS certificates during connection opening in Tackle.Connection.open/1
  # It seems to only happen on darwin systems due to the way the certificates are accessed, see here:
  # https://github.com/erlang/otp/blob/0f4345094b9a57c4166c695b7acbb6087df7e444/lib/public_key/src/pubkey_os_cacerts.erl#L133
  def handle_info({:EXIT, _port, :normal}, state) do
    {:noreply, state}
  end

  # Close channel on exit
  def terminate(reason, %{channel: channel, consumer_tag: consumer_tag}) do
    unless is_nil(consumer_tag) do
      AMQP.Basic.cancel(channel, consumer_tag)
    end

    unless is_nil(channel) do
      Tackle.Channel.close(channel)
    end

    reason
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
