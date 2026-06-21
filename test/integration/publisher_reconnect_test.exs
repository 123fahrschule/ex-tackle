defmodule Tackle.PublisherReconnectTest do
  @moduledoc """
  The publisher must recover from a lost connection: reconnect, re-declare its
  cached exchanges, and resume publishing.
  """
  use ExUnit.Case

  require Support

  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  defmodule ReconnectConsumer do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.publisher-reconnect-exchange",
      routing_key: "reconnect",
      service: "ex-tackle.publisher-reconnect-service",
      connection_id: :reconnect_consumer,
      retry_limit: 0

    def handle_message(message) do
      Application.put_env(:tackle, :publisher_reconnect_message, message)
      :ok
    end
  end

  setup do
    name = :"publisher_reconnect_#{System.unique_integer([:positive])}"
    Support.cleanup!(ReconnectConsumer)
    Application.delete_env(:tackle, :publisher_reconnect_message)

    on_exit(fn ->
      Support.stop_publisher(name)
      Support.cleanup!(ReconnectConsumer)
    end)

    {:ok, pid} = ReconnectConsumer.start_link()
    Support.wait_consumer_ready(pid)

    %{name: name}
  end

  test "recovers and resumes publishing after the connection is dropped", %{name: name} do
    options = %{
      rabbitmq_url: @rabbitmq_url,
      exchange: "ex-tackle.publisher-reconnect-exchange",
      routing_key: "reconnect",
      publisher_connection_name: name
    }

    assert :ok = Tackle.publish("before", options)

    Support.wait_until(5_000, fn ->
      assert Application.get_env(:tackle, :publisher_reconnect_message) == "before"
    end)

    # Drop the publisher's underlying connection.
    connection = Keyword.fetch!(Tackle.Connection.get_all(), name)
    Process.exit(connection.pid, :kill)

    # Publishing resumes once the publisher has reconnected and re-declared the
    # exchange on the new connection.
    Support.wait_until(10_000, fn ->
      assert :ok == Tackle.publish("after", options)
      assert Application.get_env(:tackle, :publisher_reconnect_message) == "after"
    end)
  end
end
