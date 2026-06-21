# Tackle

Tackles the problem of processing asynchronous jobs in reliable manner
by relying on RabbitMQ.

You should also take a look at [Ruby Tackle](https://github.com/renderedtext/tackle).

## Why should I use tackle?

- It is ideal for fast microservice prototyping
- It uses sane defaults for queue and exchange creation
- It retries messages that fail to be processed
- It stores unprocessed messages into a __dead__ queue for later inspection

## Installation

Add the following to the list of your dependencies:

``` elixir
def deps do
  [
    {:tackle, github: "STUDITEMPS/ex-tackle"}
  ]
end
```


## Publishing messages to an exchange

To publish a message to an exchange:

``` elixir
options = %{
  rabbitmq_url: "amqp://localhost",
  exchange: "test-exchange",
  routing_key: "test-messages",
}

Tackle.publish("Hi!", options)
```

## Consuming messages from an exchange

![Tackle Consumer Topology](https://raw.githubusercontent.com/STUDITEMPS/ex-tackle/master/topology.png)

First, declare a consumer module:

``` elixir
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

``` elixir
TestConsumer.start_link()
```

### Further options for the consumer are:
* `retry` is either `false` or a list `[delay: 3, limit: 3]`. If you don't specify a value or supply `true`,
  the default values `[delay: 10, limit: 10]` are used.
* `prefetch_count` specifies the number of messages pulled from RabbitMQ at once. Default is `1`
* `connection_id` is a string. If you use the same value for all of your consumers, only 1 RabbitMQ connection
  will be opened and used. Default value is `:default` meaning that you always use a new RabbitMQ connection.

## Handling Errors
If your consumer cannot process a message and your consumer crashes, you can use an `on_error` callback, so you have
the chance to e.g. log the error

``` elixir
def on_error(payload, message_metadata, {error_reason, stacktrace}, current_attempt, max_number_of_attempts) do
    Logger.info("An error #{error_reason} occurred.")
  end
```

Don't get confused: `max_number_of_attempts` is `retry_limit + 1` and not equal to `retry_limit`. The same applies to
the `current_attempt` value.

`on_error/5` is invoked for *every* failed attempt. If you only want to act once retries are exhausted (e.g. report the
final error to Sentry), implement the `on_retries_exhausted/3` callback instead of comparing `current_attempt` with
`max_number_of_attempts` yourself. It is called exactly once, right after the last attempt failed and the message is
routed to the dead queue:

``` elixir
def on_retries_exhausted(payload, message_metadata, {error_reason, stacktrace}) do
  Sentry.capture_exception(error_reason, stacktrace: stacktrace, extra: %{payload: payload})
end
```

If you want to retry a message processing without raising an error, your consumer's `handle_message` can throw an
`{:retry, retry_reason}`. Then the message gets pushed to the retry queue as usual.

``` elixir
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
