defmodule Tackle.PublisherPoolingTest do
  @moduledoc """
  Regression test for Finding 4 ("a new connection per publish/retry").

  Publishing many messages through the pooled publisher must open only a single,
  bounded connection — verified via the RabbitMQ management API — instead of one
  connection per message.
  """
  use ExUnit.Case

  require Support

  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  describe "pooled publishing" do
    setup do
      name = :"pooling_#{System.unique_integer([:positive])}"
      exchange = "ex-tackle.pooling-test-exchange"

      on_exit(fn ->
        Support.stop_publisher(name)
        Support.delete_exchange(exchange)
      end)

      %{name: name, exchange: exchange}
    end

    test "publishing N messages opens only one connection", %{name: name, exchange: exchange} do
      options = %{
        rabbitmq_url: @rabbitmq_url,
        exchange: exchange,
        routing_key: "pooling",
        publisher_connection_name: name
      }

      assert Support.rabbitmq_connection_count(name) == 0

      for i <- 1..50 do
        assert :ok = Tackle.publish("message #{i}", options)
      end

      # All 50 publishes share the single pooled connection.
      Support.wait_until(5_000, fn ->
        assert Support.rabbitmq_connection_count(name) == 1
      end)

      # A second burst still reuses the same single connection.
      for i <- 51..100 do
        assert :ok = Tackle.publish("message #{i}", options)
      end

      Support.wait_until(5_000, fn ->
        assert Support.rabbitmq_connection_count(name) == 1
      end)
    end
  end
end
