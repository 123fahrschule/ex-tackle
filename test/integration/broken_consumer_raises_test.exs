defmodule Tackle.BrokenConsumerRaisesTest do
  use ExSpec

  alias Support.MessageTrace

  defmodule BrokenConsumer do
    use Tackle.Consumer,
      rabbitmq_url: "amqp://localhost",
      remote_exchange: "ex-tackle.test-exchange",
      routing_key: "test-messages",
      service: "ex-tackle.broken-service-raise",
      retry_delay: 1,
      retry_limit: 3

    def handle_message(message) do
      message |> MessageTrace.save("broken-service-raise")

      raise "Raised!"
    end
  end

  @publish_options %{
    rabbitmq_url: "amqp://localhost",
    remote_exchange: "ex-tackle.test-exchange",
    routing_key: "test-messages"
  }

  setup do
    reset_test_exchanges_and_queues()

    on_exit(fn ->
      reset_test_exchanges_and_queues()
    end)

    MessageTrace.clear("broken-service-raise")

    {:ok, _} = BrokenConsumer.start_link()

    :timer.sleep(1000)
  end

  describe "healthy consumer" do
    it "receives the message multiple times" do
      Tackle.publish("Hi!", @publish_options)

      :timer.sleep(5000)

      assert MessageTrace.content("broken-service-raise") == "Hi!Hi!Hi!Hi!"
    end
  end

  defp reset_test_exchanges_and_queues do
    Support.delete_all_queues("ex-tackle.broken-service-raise.test-messages")

    Support.delete_exchange("ex-tackle.broken-service-raise.test-messages")
    Support.delete_exchange("ex-tackle.test-exchange")
  end
end
