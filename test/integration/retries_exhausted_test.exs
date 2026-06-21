defmodule Tackle.RetriesExhaustedTest do
  use ExUnit.Case

  require Support
  alias Support.MessageTrace

  defmodule ExhaustingConsumer do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    # Exposed via functions so the test reads the very same trace names the
    # callbacks write to (module attributes are scoped to this module only).
    @on_error_trace "retries-exhausted.on-error"
    @exhausted_trace "retries-exhausted.on-retries-exhausted"
    def on_error_trace, do: @on_error_trace
    def exhausted_trace, do: @exhausted_trace

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.test-exchange",
      routing_key: "test-messages",
      service: "ex-tackle.retries-exhausted-service",
      retry_delay: 1,
      retry_limit: 3

    def handle_message(_message) do
      # exception without warning
      Code.eval_quoted(quote do: :a + 1)
    end

    # Fires for every failed attempt.
    def on_error(_payload, _message_metadata, _error, current_attempt, max_number_of_attempts) do
      MessageTrace.save("on_error:#{current_attempt}/#{max_number_of_attempts};", @on_error_trace)
    end

    # Must fire exactly once, after the last attempt failed.
    def on_retries_exhausted(payload, _message_metadata, {error_reason, _stacktrace}) do
      MessageTrace.save("exhausted:#{payload}:#{inspect(error_reason.__struct__)};", @exhausted_trace)
    end
  end

  @publish_options %{
    rabbitmq_url: Application.compile_env(:tackle, :rabbitmq_url),
    exchange: "ex-tackle.test-exchange",
    routing_key: "test-messages"
  }

  setup do
    Support.cleanup!(ExhaustingConsumer)

    on_exit(fn ->
      Support.cleanup!(ExhaustingConsumer)
    end)

    MessageTrace.clear(ExhaustingConsumer.on_error_trace())
    MessageTrace.clear(ExhaustingConsumer.exhausted_trace())

    {:ok, pid} = ExhaustingConsumer.start_link()

    Support.wait_consumer_ready(pid)

    {:ok, %{pid: pid}}
  end

  describe "consumer that fails on every attempt" do
    test "calls on_retries_exhausted exactly once, after the final attempt" do
      Tackle.publish("Hi!", @publish_options)

      # retry_limit: 3 + retry_delay: 1 => 4 attempts spread over ~3s
      Support.wait_until(8000, fn ->
        assert MessageTrace.read(ExhaustingConsumer.on_error_trace()) ==
                 "on_error:1/4;on_error:2/4;on_error:3/4;on_error:4/4;"
      end)

      # on_retries_exhausted fired exactly once, on the final attempt. It runs
      # asynchronously right after the last on_error, so wait for it too.
      Support.wait_until(2000, fn ->
        assert MessageTrace.read(ExhaustingConsumer.exhausted_trace()) ==
                 "exhausted:Hi!:ArithmeticError;"
      end)
    end
  end
end
