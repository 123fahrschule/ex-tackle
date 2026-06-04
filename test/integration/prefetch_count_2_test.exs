defmodule Tackle.ParallelMessageHandling_2_Test do
  use ExUnit.Case

  defmodule TestConsumer do
    @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

    use Tackle.Consumer,
      rabbitmq_url: @rabbitmq_url,
      remote_exchange: "ex-tackle.test-prefetch-2-exchange",
      routing_key: "prefetch",
      service: "ex-tackle.prefetch-count-service",
      prefetch_count: 2

    def handle_message(message) do
      "#PID" <> spid = message
      sup = spid |> String.to_charlist() |> :erlang.list_to_pid()
      Task.Supervisor.async_nolink(sup, fn -> :timer.sleep(:infinity) end)

      receive do
        _msg -> nil
      end
    end
  end

  @publish_options %{
    rabbitmq_url: Application.compile_env(:tackle, :rabbitmq_url),
    exchange: "ex-tackle.test-prefetch-2-exchange",
    routing_key: "prefetch"
  }

  setup do
    Support.cleanup!(TestConsumer)

    on_exit(fn ->
      Support.cleanup!(TestConsumer)
    end)

    {:ok, _} = TestConsumer.start_link()

    {:ok, sup} = Task.Supervisor.start_link()

    :timer.sleep(1000)

    {:ok, [sup: sup]}
  end

  describe "parallel message handling" do
    test "handles messages in pairs", context do
      sup = context[:sup]
      Tackle.publish(sup |> inspect(), @publish_options)
      Tackle.publish(sup |> inspect(), @publish_options)
      Tackle.publish(sup |> inspect(), @publish_options)
      Tackle.publish(sup |> inspect(), @publish_options)

      :timer.sleep(1000)

      assert Task.Supervisor.children(sup) |> Enum.count() == 2

      Task.Supervisor.children(sup)
      |> Enum.each(fn pid -> Task.Supervisor.terminate_child(sup, pid) end)

      :timer.sleep(1000)

      assert Task.Supervisor.children(sup) |> Enum.count() == 2

      Task.Supervisor.children(sup)
      |> Enum.each(fn pid -> Task.Supervisor.terminate_child(sup, pid) end)

      :timer.sleep(1000)

      assert Task.Supervisor.children(sup) |> Enum.count() == 0
    end
  end
end
