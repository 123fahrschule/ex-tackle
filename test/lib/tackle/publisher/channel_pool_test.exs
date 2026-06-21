defmodule Tackle.Publisher.ChannelPoolTest do
  use ExUnit.Case

  alias Tackle.Publisher.ChannelPool

  @rabbitmq_url Application.compile_env(:tackle, :rabbitmq_url)

  setup do
    {:ok, connection} = AMQP.Connection.open(@rabbitmq_url)

    on_exit(fn ->
      if Process.alive?(connection.pid), do: AMQP.Connection.close(connection)
    end)

    %{connection: connection}
  end

  defp start_pool(connection, opts) do
    pool_size = Keyword.get(opts, :pool_size, 2)
    confirms? = Keyword.get(opts, :confirms?, false)

    {:ok, pool} =
      NimblePool.start_link(
        worker: {ChannelPool, %{connection: connection, confirms?: confirms?}},
        pool_size: pool_size
      )

    on_exit(fn -> if Process.alive?(pool), do: NimblePool.stop(pool, :shutdown, 1_000) end)
    pool
  end

  test "checks out a live channel and returns it to the pool", %{connection: connection} do
    pool = start_pool(connection, pool_size: 1)

    channel_pid =
      NimblePool.checkout!(pool, :checkout, fn _from, channel ->
        assert Process.alive?(channel.pid)
        {channel.pid, channel}
      end)

    # The same channel comes back out and is still alive (it was checked in,
    # not closed).
    channel_pid_again =
      NimblePool.checkout!(pool, :checkout, fn _from, channel ->
        {channel.pid, channel}
      end)

    assert channel_pid_again == channel_pid
    assert Process.alive?(channel_pid)
  end

  test "with confirms enabled, channels are in confirm mode", %{connection: connection} do
    pool = start_pool(connection, pool_size: 1, confirms?: true)

    result =
      NimblePool.checkout!(pool, :checkout, fn _from, channel ->
        :ok = AMQP.Basic.publish(channel, "", "tackle-channel-pool-test-#{System.unique_integer()}", "hi")
        {AMQP.Confirm.wait_for_confirms(channel, 1_000), channel}
      end)

    # wait_for_confirms only works on a channel in confirm mode.
    assert result == true
  end

  test "a dead channel is removed and replaced on the next checkout", %{connection: connection} do
    pool = start_pool(connection, pool_size: 1)

    # Close the channel from within the checkout, so a dead channel is checked
    # back into the pool (a graceful close leaves the connection intact).
    first_pid =
      NimblePool.checkout!(pool, :checkout, fn _from, channel ->
        :ok = AMQP.Channel.close(channel)
        {channel.pid, channel}
      end)

    refute Process.alive?(first_pid)

    second_pid =
      NimblePool.checkout!(pool, :checkout, fn _from, channel -> {channel.pid, channel} end)

    assert second_pid != first_pid
    assert Process.alive?(second_pid)
  end
end
