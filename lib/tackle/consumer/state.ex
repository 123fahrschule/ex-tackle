defmodule Tackle.Consumer.State do
  @enforce_keys [
    :handler,
    :topology,
    :rabbitmq_url,
    :connection_id,
    :prefetch_count
  ]
  defstruct [
    :handler,
    :topology,
    :channel,
    :rabbitmq_url,
    :consumer_tag,
    :channel_retry_ref,
    :consume_retry_ref,
    reconnect_interval: 1_000,
    connection_id: :default,
    prefetch_count: 1,
    publisher: Tackle.DelayedRetry
  ]

  def configure!(options) do
    options = Map.put_new(options, :topology, Tackle.Consumer.Topology.from_options!(options))

    struct(__MODULE__, options)
  end

end
