defmodule Tackle.Publisher.ChannelPool do
  @moduledoc """
  A `NimblePool` of AMQP channels that all live on a single, long-lived
  connection owned by a `Tackle.Publisher`.

  Channels are cheap but must not be shared across processes, so each publish
  checks a channel out of the pool, uses it in the calling process, and checks
  it back in. When publisher confirms are enabled the channels are put into
  confirm mode once, at worker init.

  The pool itself is started and torn down by the owning `Tackle.Publisher`:
  when the connection is lost the publisher stops the pool and starts a fresh
  one against the new connection.

  Channels are always put into confirm mode: enabling confirms is cheap, and it
  lets publisher confirms be toggled per publish (the publisher only *waits* for
  a confirm when asked). Fire-and-forget publishes simply ignore the acks.
  """

  @behaviour NimblePool

  require Logger

  @impl NimblePool
  def init_worker(%{connection: connection} = pool_state) do
    {:ok, channel} = AMQP.Channel.open(connection)
    :ok = AMQP.Confirm.select(channel)

    {:ok, channel, pool_state}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, channel, pool_state) do
    if Process.alive?(channel.pid) do
      {:ok, channel, channel, pool_state}
    else
      {:remove, :closed, pool_state}
    end
  end

  @impl NimblePool
  def handle_checkin(_client_state, _from, channel, pool_state) do
    if Process.alive?(channel.pid) do
      {:ok, channel, pool_state}
    else
      {:remove, :closed, pool_state}
    end
  end

  @impl NimblePool
  def terminate_worker(_reason, channel, pool_state) do
    if Process.alive?(channel.pid) do
      # Best effort: the connection may already be gone during shutdown.
      try do
        AMQP.Channel.close(channel)
      catch
        _kind, _reason -> :ok
      end
    end

    {:ok, pool_state}
  end
end
