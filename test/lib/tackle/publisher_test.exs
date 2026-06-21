defmodule Tackle.PublisherTest do
  use ExUnit.Case

  require Support

  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  describe "interpret_confirm/2 (pure)" do
    test "maps confirm outcomes to publish results" do
      assert Tackle.Publisher.interpret_confirm(true, false) == :ok
      assert Tackle.Publisher.interpret_confirm(false, false) == {:error, :nack}
      assert Tackle.Publisher.interpret_confirm(:timeout, false) == {:error, :confirm_timeout}
      # A returned (unroutable) message wins regardless of the confirm outcome.
      assert Tackle.Publisher.interpret_confirm(true, true) == {:error, :unroutable}
      assert Tackle.Publisher.interpret_confirm(:timeout, true) == {:error, :unroutable}
    end
  end

  describe "declare-once caching" do
    setup do
      name = :"declare_once_#{System.unique_integer([:positive])}"
      exchange = "ex-tackle.publisher-declare-test"

      on_exit(fn ->
        Support.stop_publisher(name)
        Support.delete_exchange(exchange)
      end)

      %{name: name, exchange: exchange}
    end

    test "declares an exchange once and caches it", %{name: name, exchange: exchange} do
      refute Tackle.Publisher.declared?(name, exchange, :direct)

      assert :ok =
               Tackle.Publisher.publish(name, @rabbitmq_url, exchange, "rk", "first")

      assert Tackle.Publisher.declared?(name, exchange, :direct)

      # A second publish finds the exchange already cached (no re-declare).
      assert :ok =
               Tackle.Publisher.publish(name, @rabbitmq_url, exchange, "rk", "second")

      assert Tackle.Publisher.declared?(name, exchange, :direct)
    end
  end

  describe "publisher confirms" do
    setup do
      name = :"confirms_#{System.unique_integer([:positive])}"
      exchange = "ex-tackle.publisher-confirms-test"
      queue = "ex-tackle.publisher-confirms-test.q"

      # Routable target: a queue bound to the exchange on routing key "ok".
      Support.create_exchange(exchange)

      Tackle.execute(@rabbitmq_url, :default, fn channel ->
        {:ok, _} = AMQP.Queue.declare(channel, queue, durable: true)
        Tackle.Exchange.bind_to_queue(channel, exchange, queue, "ok")
      end)

      on_exit(fn ->
        Support.stop_publisher(name)
        Support.delete_queue(queue)
        Support.delete_exchange(exchange)
      end)

      %{name: name, exchange: exchange}
    end

    test "a routable message is confirmed", %{name: name, exchange: exchange} do
      assert :ok =
               Tackle.Publisher.publish(name, @rabbitmq_url, exchange, "ok", "hello",
                 confirm: true
               )
    end

    test "an unroutable message returns an error", %{name: name, exchange: exchange} do
      # No queue is bound to routing key "nobody" -> mandatory publish returns.
      assert {:error, :unroutable} =
               Tackle.Publisher.publish(name, @rabbitmq_url, exchange, "nobody", "hello",
                 confirm: true
               )
    end
  end

  describe "reconnect" do
    setup do
      name = :"reconnect_#{System.unique_integer([:positive])}"
      exchange = "ex-tackle.publisher-reconnect-test"

      on_exit(fn ->
        Support.stop_publisher(name)
        Support.delete_exchange(exchange)
      end)

      %{name: name, exchange: exchange}
    end

    test "invalidates the declare cache and re-declares on reconnect", %{
      name: name,
      exchange: exchange
    } do
      assert :ok = Tackle.Publisher.publish(name, @rabbitmq_url, exchange, "rk", "before")
      assert Tackle.Publisher.declared?(name, exchange, :direct)

      # Drop the underlying connection.
      [{_pid, _}] = Registry.lookup(Tackle.Publisher.Registry, name)
      connection = Keyword.fetch!(Tackle.Connection.get_all(), name)
      Process.exit(connection.pid, :kill)

      # The publisher clears its cache when it notices the connection is gone.
      Support.wait_until(5_000, fn ->
        refute Tackle.Publisher.declared?(name, exchange, :direct)
      end)

      # ...and publishing resumes (re-declaring the exchange on the new connection).
      Support.wait_until(5_000, fn ->
        assert :ok == Tackle.Publisher.publish(name, @rabbitmq_url, exchange, "rk", "after")
      end)

      assert Tackle.Publisher.declared?(name, exchange, :direct)
    end
  end
end
