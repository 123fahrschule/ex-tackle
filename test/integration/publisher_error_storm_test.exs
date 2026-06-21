defmodule Tackle.PublisherErrorStormTest do
  @moduledoc """
  The retry/dead path must reuse a connection too: during an error storm, the
  shared pooled publisher (`:tackle_publisher`) must stay at a single bounded
  connection instead of opening one per retried message.
  """
  use ExUnit.Case

  require Support

  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  defmodule StormConsumer do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.storm-test-exchange",
      routing_key: "storm",
      service: "ex-tackle.storm-service",
      connection_id: :storm_consumer,
      retry_delay: 1,
      retry_limit: 2

    def handle_message(_message) do
      raise "always fails"
    end
  end

  @publish_options %{
    rabbitmq_url: @rabbitmq_url,
    exchange: "ex-tackle.storm-test-exchange",
    routing_key: "storm"
  }

  @publisher_name Tackle.default_publisher_name()

  setup do
    Support.cleanup!(StormConsumer)

    on_exit(fn ->
      Support.cleanup!(StormConsumer)
    end)

    {:ok, _} = StormConsumer.start_link()
    :timer.sleep(1000)
    :ok
  end

  test "connection count stays bounded while messages fail and retry" do
    # Publish a burst of messages that will all fail and be retried through the
    # delay/dead path.
    for i <- 1..20 do
      assert :ok = Tackle.publish("storm #{i}", @publish_options)
    end

    # Sample the shared publisher's connection count repeatedly during the storm
    # and assert it never exceeds one.
    for _ <- 1..10 do
      assert Support.rabbitmq_connection_count(@publisher_name) <= 1
      :timer.sleep(500)
    end
  end
end
