defmodule Tackle.SharedConnection.Test do
  use ExUnit.Case
  require Support

  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  defmodule TestConsumer1 do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.test-multiple-channels-exchange-1",
      routing_key: "multiple-channels",
      service: "ex-tackle.multiple-channels-service-1",
      connection_id: :single_connection

    def handle_message(message) do
      Application.put_env(:tackle, :shared_connection_test_consumer_1_message, message)
    end
  end

  defmodule TestConsumer2 do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.test-multiple-channels-exchange-2",
      routing_key: "multiple-channels",
      service: "ex-tackle.multiple-channels-service-2",
      connection_id: :single_connection

    def handle_message(message) do
      Application.put_env(:tackle, :shared_connection_test_consumer_2_message, message)
    end
  end

  def message_handler(message, response) do
    "#PID" <> pid = message
    client = pid |> String.to_charlist() |> :erlang.list_to_pid()
    send(client, response)
  end

  @publish_options_1 %{
    rabbitmq_url: @rabbitmq_url,
    exchange: "ex-tackle.test-multiple-channels-exchange-1",
    routing_key: "multiple-channels"
  }

  @publish_options_2 %{
    rabbitmq_url: @rabbitmq_url,
    exchange: "ex-tackle.test-multiple-channels-exchange-2",
    routing_key: "multiple-channels"
  }

  setup_all do
    # Forget all opened connections
    Process.whereis(Tackle.Connection) |> Process.exit(:kill)
    :timer.sleep(100)
  end

  setup do
    Support.cleanup!(TestConsumer1)
    Support.cleanup!(TestConsumer2)

    on_exit(fn ->
      Support.cleanup!(TestConsumer1)
      Support.cleanup!(TestConsumer2)
    end)
  end

  describe "shared connection" do
    test "- reopen consumers" do
      {:ok, c1} = TestConsumer1.start_link()
      {:ok, c2} = TestConsumer2.start_link()
      Support.wait_consumer_ready(c1)
      Support.wait_consumer_ready(c2)

      # only one connection opend
      assert Tackle.Connection.get_all() |> Enum.count() == 1

      verify_consumer_functionality()

      # kill consumers
      Process.unlink(c1)
      Process.unlink(c2)
      Process.exit(c1, :kill)
      Process.exit(c2, :kill)

      # kill connection process
      assert Tackle.Connection.get_all() |> Enum.count() == 1
      old_pid = get_all_connections()
      old_pid |> Process.exit(:kill)

      # restart consumers
      {:ok, c1} = TestConsumer1.start_link()
      {:ok, c2} = TestConsumer2.start_link()
      Support.wait_consumer_ready(c1)
      Support.wait_consumer_ready(c2)

      # new connection process?
      assert Tackle.Connection.get_all() |> Enum.count() == 1
      new_pid = get_all_connections()
      assert old_pid != new_pid

      verify_consumer_functionality()
    end

    def verify_consumer_functionality do
      me_pid_string = self() |> inspect()
      Tackle.publish(me_pid_string, @publish_options_1)
      Tackle.publish(me_pid_string, @publish_options_2)

      Support.wait_until(5_000, fn ->
        assert Application.get_env(:tackle, :shared_connection_test_consumer_1_message) == me_pid_string
      end)
      Support.wait_until(5_000, fn ->
        assert Application.get_env(:tackle, :shared_connection_test_consumer_2_message) == me_pid_string
      end)
    end

    def get_all_connections do
      Tackle.Connection.get_all() |> Keyword.get(:single_connection) |> Map.get(:pid)
    end

    def rcv do
      receive do
        msg -> msg
      end
    end
  end
end
