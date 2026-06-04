defmodule Tackle.BrokenConsumerTest do
  use ExUnit.Case

  alias Support.MessageTrace

  defmodule BrokenConsumer do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.test-exchange",
      routing_key: "test-messages",
      service: "ex-tackle.broken-service",
      retry_delay: 1,
      retry_limit: 3

    def handle_message(message) do
      message |> MessageTrace.save("broken-service")

      # exception without warning
      Code.eval_quoted(quote do: :a + 1)
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

    MessageTrace.clear("broken-service")

    {:ok, _} = BrokenConsumer.start_link()

    :timer.sleep(1000)
  end

  describe "healthy consumer" do
    test "receives the message multiple times" do
      Tackle.publish("Hi!", @publish_options)

      :timer.sleep(5000)

      assert MessageTrace.content("broken-service") == "Hi!Hi!Hi!Hi!"
    end
  end
end
