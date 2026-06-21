defmodule Tackle.DelayedRetry do
  use AMQP
  require Logger

  def retry_count_from_headers(:undefined), do: 0
  def retry_count_from_headers([]), do: 0
  def retry_count_from_headers([{"retry_count", :long, count} | _tail]), do: count
  def retry_count_from_headers([_ | tail]), do: retry_count_from_headers(tail)

  def publish(rabbitmq_url, queue, payload, message_options) do
    # Route through the shared pooled publisher (default exchange, no declare)
    # so an error storm reuses a single connection instead of opening a fresh
    # one per retried message. The `retry_count` header in `message_options` is
    # preserved.
    Tackle.Publisher.publish(
      Tackle.default_publisher_name(),
      rabbitmq_url,
      "",
      queue,
      payload,
      message_options
    )
  end
end
