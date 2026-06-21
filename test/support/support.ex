defmodule Support do
  alias Support.RabbitmqAPI
  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  defmacro start_consumer!(consumer) do
    quote do
      pid = start_supervised!(unquote(consumer))
      assert Process.alive?(pid)

      Support.wait_consumer_ready(pid)
      pid
    end
  end

  defmacro wait_consumer_ready(pid) do
    quote do
      Support.wait_until(fn ->
        %{consumer_tag: consumer_tag} = :sys.get_state(unquote(pid))
        refute is_nil(consumer_tag)
      end)
    end
  end

  def rabbitmq_list_exchanges() do
    RabbitmqAPI.list_exchanges().body |> Enum.map(fn %{"name" => name} -> name end)
  end

  def rabbitmq_list_queues() do
    RabbitmqAPI.list_queues().body |> Enum.map(fn %{"name" => name} -> name end)
  end

  @doc """
  All AMQP connection names (the client-provided `connection_name`) currently
  open on the broker, as reported by the management API.
  """
  def rabbitmq_connection_names() do
    RabbitmqAPI.list_connections().body
    |> Enum.map(fn conn -> get_in(conn, ["client_properties", "connection_name"]) end)
  end

  @doc """
  How many connections with the given `user_provided_name` are open.

  The management API stats lag slightly behind reality, so assertions on this
  value should be wrapped in `wait_until/2`.
  """
  def rabbitmq_connection_count(name) do
    name = to_string(name)

    rabbitmq_connection_names()
    |> Enum.count(&(&1 == name))
  end

  @doc """
  Stop the pooled publisher registered under `name` (if any) and close its
  cached connection, so a following test starts from a clean slate.
  """
  def stop_publisher(name) do
    case Registry.lookup(Tackle.Publisher.Registry, name) do
      [{pid, _meta}] ->
        DynamicSupervisor.terminate_child(Tackle.Publisher.Supervisor, pid)

      _ ->
        :ok
    end

    case Keyword.get(Tackle.Connection.get_all(), name) do
      nil -> :ok
      connection -> if Process.alive?(connection.pid), do: Tackle.Connection.close(connection)
    end

    :ok
  end

  def cleanup!(consumer_module) do
    execute(fn channel ->
      topology = consumer_module.topology()
      Tackle.Consumer.Topology.cleanup!(channel, topology, _delete_remote_exchange = true)
    end)
  end

  def create_exchange(exchange_name) do
    execute(fn channel ->
      Tackle.Exchange.create(channel, exchange_name)
    end)
  end

  def delete_exchange(exchange_name) do
    execute(fn channel ->
      :ok = AMQP.Exchange.delete(channel, exchange_name)
    end)
  end

  def queue_status(queue_name) do
    {:ok, status} =
      execute(fn channel ->
        AMQP.Queue.status(channel, queue_name)
      end)

    status
  end

  def purge_queue(queue_name) do
    execute(fn channel ->
      AMQP.Queue.purge(channel, queue_name)
    end)
  end

  def delete_queue(queue_name) do
    execute(fn channel ->
      {:ok, _} = AMQP.Queue.delete(channel, queue_name)
    end)
  end

  def delete_all_queues(queue_name, delay \\ 1) do
    execute(fn channel ->
      AMQP.Queue.delete(channel, queue_name)
      AMQP.Queue.delete(channel, queue_name <> ".dead")
      AMQP.Queue.delete(channel, queue_name <> ".delay.#{delay}")
    end)
  end

  defp execute(fun) when is_function(fun, 1) do
    Tackle.execute(@rabbitmq_url, :default, fun)
  end

  def wait_until(fun), do: wait_until(500, fun)

  def wait_until(0, fun), do: fun.()

  def wait_until(timeout, fun) do
    try do
      fun.()
    rescue
      ExUnit.AssertionError ->
        :timer.sleep(100)
        wait_until(max(0, timeout - 100), fun)
    end
  end
end
