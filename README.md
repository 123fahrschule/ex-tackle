# Tackle

Tackles the problem of processing asynchronous jobs in reliable manner
by relying on RabbitMQ.

You should also take a look at [Ruby Tackle](https://github.com/renderedtext/tackle).

## Why should I use tackle?

- It is ideal for fast microservice prototyping
- It uses sane defaults for queue and exchange creation
- It retries messages that fail to be processed
- It stores unprocessed messages into a **dead** queue for later inspection

## Installation

Add the following to the list of your dependencies:

```elixir
def deps do
  [
    {:tackle, github: "STUDITEMPS/ex-tackle"}
  ]
end
```

## Publishing messages to an exchange

To publish a message to an exchange:

```elixir
options = %{
  rabbitmq_url: "amqp://localhost",
  exchange: "test-exchange",
  routing_key: "test-messages",
}

Tackle.publish("Hi!", options)
```

`Tackle.publish/2` returns `:ok` or `{:error, reason}`.

### How publishing works

Publishing routes through a long-lived, **supervised publisher**. There is one
publisher process per connection name; it owns a persistent AMQP connection and
a small pool of channels (via [`nimble_pool`](https://hex.pm/packages/nimble_pool)).
Publishing checks a channel out of the pool and runs in the calling process, so
ex-tackle does **not** open a connection (and re-declare the exchange) per
message anymore. Each exchange is declared only once per
`{connection, exchange, type}`; the cache is invalidated and the exchange is
re-declared after a reconnect.

Publishers are started lazily on first publish, so your application boots even
when the broker is temporarily unavailable. If the connection is lost, the
publisher reconnects with a backoff and re-declares its exchanges.

### Sharing a publisher connection (`publisher_connection_name`)

Analogous to the consumer `connection_id`, `publisher_connection_name` selects
which publisher (and therefore which connection) a publish uses. All publishes
that share a name share a single connection:

```elixir
options = %{
  rabbitmq_url: "amqp://localhost",
  exchange: "test-exchange",
  routing_key: "test-messages",
  publisher_connection_name: "My Service Publisher"
}

Tackle.publish("Hi!", options)
```

If you do not supply `publisher_connection_name`, a shared default publisher is
used. The retry/dead-letter path uses this same shared default publisher, so
even an error storm reuses a single connection instead of opening one per
retried message.

### Tuning the channel pool

Each publisher keeps a small pool of channels. Channels multiplex over the one
connection, so the pool size bounds how many publishes can run truly
concurrently before they serialize. The default is `4`:

```elixir
# config/config.exs
config :tackle, publisher_pool_size: 8
```

### Publisher confirms

By default, publishing is fire-and-forget (`:ok` once handed to the channel).
You can enable [publisher confirms](https://www.rabbitmq.com/confirms.html) so a
publish waits for the broker to acknowledge the message and reports an error
instead of silently dropping it. This adds latency, so it is **off by default**
and configurable globally or per call:

```elixir
# Enable globally
config :tackle,
  publisher_confirms: true,
  publisher_confirm_timeout: 5_000
```

```elixir
# ...or per publish (overrides the global setting)
Tackle.publish("Hi!", Map.put(options, :confirm, true))
```

With confirms enabled, `Tackle.publish/2` returns:

- `:ok` — the broker confirmed the message,
- `{:error, :unroutable}` — no queue was bound for the routing key,
- `{:error, :nack}` — the broker negatively acknowledged the message,
- `{:error, :confirm_timeout}` — no confirm arrived within the timeout.

### The legacy per-call behaviour (`:default`)

Before the pooled publisher, every publish opened and closed its own connection.
That behaviour is still available for backwards compatibility, either globally:

```elixir
config :tackle, publisher_strategy: :per_call
```

or per call by passing `publisher_connection_name: :default`. The pooled
publisher is the recommended path; the per-call path remains only as an escape
hatch and may be removed in a future release.

## Consuming messages from an exchange

![Tackle Consumer Topology](https://raw.githubusercontent.com/STUDITEMPS/ex-tackle/master/topology.png)

First, declare a consumer module:

```elixir
defmodule TestConsumer do
  use Tackle.Consumer,
    rabbitmq_url: "amqp://localhost",
    remote_exchange: "test-exchange",
    routing_key: "test-messages",
    service: "my-service"

  def handle_message(message) do
    IO.puts "A message arrived. Life is good!"

    IO.puts message
  end
end
```

And then start it to consume messages:

```elixir
TestConsumer.start_link()
```

### Further options for the consumer are:

- `retry` is either `false` or a list `[delay: 3, limit: 3]`. If you don't specify a value or supply `true`,
  the default values `[delay: 10, limit: 10]` are used.
- `prefetch_count` specifies the number of messages pulled from RabbitMQ at once. Default is `1`
- `connection_id` is a string. If you use the same value for all of your consumers, only 1 RabbitMQ connection
  will be opened and used. Default value is `:default` meaning that you always use a new RabbitMQ connection.

## Handling Errors

If your consumer cannot process a message and your consumer crashes, you can use an `on_error` callback, so you have
the chance to e.g. log the error

```elixir
def on_error(payload, message_metadata, {error_reason, stacktrace}, current_attempt, max_number_of_attempts) do
    Logger.info("An error #{error_reason} occurred.")
  end
```

Don't get confused: `max_number_of_attempts` is `retry_limit + 1` and not equal to `retry_limit`. The same applies to
the `current_attempt` value.

`on_error/5` is invoked for _every_ failed attempt. If you only want to act once retries are exhausted (e.g. report the
final error to Sentry), implement the `on_retries_exhausted/3` callback instead of comparing `current_attempt` with
`max_number_of_attempts` yourself. It is called exactly once, right after the last attempt failed and the message is
routed to the dead queue:

```elixir
def on_retries_exhausted(payload, message_metadata, {error_reason, stacktrace}) do
  Sentry.capture_exception(error_reason, stacktrace: stacktrace, extra: %{payload: payload})
end
```

If you want to retry a message processing without raising an error, your consumer's `handle_message` can throw an
`{:retry, retry_reason}`. Then the message gets pushed to the retry queue as usual.

```elixir
def handle_message(message) do
  if not_ready_to_process_message_yet(message) do
    throw {:retry, :rabbitmq_out_of_order_message}
  else
    process_message(message)
  end
end
```

## Rescuing dead messages

If you consumer is broken, or in other words raises an exception while handling
messages, your messages will end up in a dead messages queue.

To rescue those messages, you can use `MyApp.MyConsumer.retry_dead_messages(how_many)`:

The above will pull one message from the dead queue and publish it on the original message exchange
with the original routing key.

To republish multiple messages, use a bigger `how_many` number.

## Opening multiple channels through the same connection

By default each channel (consumer) opens separate connection to the server.

If you want to reduce number of opened connections from one Elixir application
to RabbitMQ server, you can map multiple channels to single connection.

Each connection can have name, supplied as optional parameter `connection_id`.
All consumers that have the same connection name share single connection.

Parameter `connection_id` is optional and if not supplied,
`connection_id` is set to `:default`.
Value `:default` has exceptional semantic: all channels with `connection_id`
set to `:default` use separate connections - one channel per `:default` connection.

#### To use this feature

In consumer specification use `connection_id` parameter:

```
defmodule Consumer do
  use Tackle.Consumer,
    rabbitmq_url: "...",
    connection_id: :connection_identifier,
    ...
```

#### Specify generated exchanges type

In your `config.exs` put:

```
config :tackle, exchange_type: :topic
```

## Migrating from per-call publishing to the pooled publisher

Earlier versions opened a new connection for every published (and every
retried) message. Publishing now routes through a long-lived, pooled publisher.

**What you need to do:** nothing — the change is backwards compatible. The
public `Tackle.publish/2` API, the `publisher_connection_name` option, the
consumer `connection_id` option, and the `Tackle.Consumer` callbacks are all
unchanged.

**What changes under the hood:**

- Publishing reuses one connection and a pool of channels instead of opening a
  connection per message.
- The exchange is declared once per publisher, not on every publish.
- The retry/dead-letter path reuses the shared publisher connection instead of
  opening a fresh connection per retried message.
- `Tackle.publish/2` still returns `:ok`, and now also returns `{:error, reason}`
  when publisher confirms are enabled and the broker does not confirm the
  message.

**Recommended settings:**

- Keep the default pooled strategy.
- Set `publisher_connection_name` per service (or per logical publisher) to make
  connections easy to identify in the RabbitMQ management UI.
- Leave publisher confirms off unless you need the delivery guarantee; enable
  them per call for the publishes that must not be silently lost.

If you must restore the old per-call behaviour, set
`config :tackle, publisher_strategy: :per_call` (global) or pass
`publisher_connection_name: :default` (per call).

## Configuration reference

All keys live under the `:tackle` application config (`config :tackle, ...`):

| Key                            | Default   | Description                                                                                                       |
| ------------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------- |
| `publisher_strategy`           | `:pooled` | `:pooled` uses the long-lived pooled publisher; `:per_call` restores the legacy connection-per-message behaviour. |
| `publisher_pool_size`          | `4`       | Number of channels per publisher.                                                                                 |
| `publisher_confirms`           | `false`   | Enable publisher confirms (wait for broker acknowledgement).                                                      |
| `publisher_confirm_timeout`    | `5_000`   | Milliseconds to wait for a confirm before returning `{:error, :confirm_timeout}`.                                 |
| `publisher_reconnect_interval` | `1_000`   | Milliseconds to wait before a publisher retries a lost connection.                                                |
| `exchange_type`                | `:direct` | Type used when declaring exchanges (`:direct`, `:topic`, `:fanout`, `:headers`, `:match`).                        |
