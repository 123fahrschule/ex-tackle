defmodule Tackle.BrokenConsumerRepublishesMessageTest do
  use ExSpec

  alias Support.MessageTrace

  defmodule BrokenPublisher do
    defdelegate retry_count_from_headers(headers),
      to: Tackle.DelayedRetry,
      as: :retry_count_from_headers

    def publish(_rabbitmq_url, _queue, _payload, _message_options) do
      Code.eval_quoted(quote do: {:ok, _connection} = {:error, :econnrefused})
    end
  end

  defmodule BrokenConsumer do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url:  @rabbitmq_url,
      remote_exchange: "ex-tackle.test-exchange",
      routing_key: "test-messages",
      service: "ex-tackle.broken-service-raise",
      # NOTE(kklisura): The publisher module simulates failed publishing
      publisher: Tackle.BrokenConsumerRepublishesMessageTest.BrokenPublisher,
      retry_delay: 1,
      retry_limit: 1

    def handle_message(message) do
      message |> MessageTrace.save("broken-service-raise")

      raise "Raised!"
    end
  end

  @publish_options %{
    rabbitmq_url: Application.compile_env(:tackle, :rabbitmq_url),
    exchange: "ex-tackle.test-exchange",
    routing_key: "test-messages"
  }

  setup do
    Support.cleanup!(BrokenConsumer)

    on_exit(fn ->
      Support.cleanup!(BrokenConsumer)
    end)

    MessageTrace.clear("broken-service-raise")

    {:ok, _} = BrokenConsumer.start_link()

    :timer.sleep(1000)
  end

  describe "healthy consumer" do
    it "receives the message multiple times" do
      Tackle.publish("Hi!", @publish_options)
      Tackle.publish("There!", @publish_options)

      :timer.sleep(5000)

      # NOTE(kklisura): There! message never gets processed. The process hangs.
      assert MessageTrace.content("broken-service-raise") == "Hi!There!Hi!There!"
    end
  end
end
