defmodule Tackle do
  use Application

  require Logger

  # Reserved connection name for the shared, default pooled publisher. Used when
  # no `publisher_connection_name` is given and for the retry/dead/republish path.
  @default_publisher_name :tackle_publisher

  @doc false
  def default_publisher_name, do: @default_publisher_name

  @impl Application
  def start(_type, _args) do
    children = [
      Tackle.Connection,
      {Registry, keys: :unique, name: Tackle.Publisher.Registry},
      Tackle.Publisher.Supervisor
    ]

    opts = [strategy: :one_for_one, name: Tackle.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # FIXME: why do we use options here? We need all of them, so make them mandatory
  # FIXME: this function is too generic if you use `exchange` as the option. Generic publishing should be done
  # with the AMQP.Basic.publish function. Here, we should enforce the `tackle` behaviour which publishes all messages
  # over the applications own __service_exchange__. So rename the option name!!!
  @doc """
  Publish `message` to the given exchange and routing key.

  By default this routes through a long-lived, supervised `Tackle.Publisher`
  that owns a persistent connection and a small channel pool, so publishing no
  longer opens a connection (and re-declares the exchange) per message.

  Options:

    * `:rabbitmq_url` (required) — broker URL.
    * `:exchange` (required) — target exchange.
    * `:routing_key` (required) — routing key.
    * `:publisher_connection_name` — name of the publisher/connection to use.
      Publishes sharing a name share a single connection. Defaults to a shared
      pooled publisher. Pass `:default` to fall back to the legacy per-call
      connection behaviour.
    * `:confirm` — override the global publisher-confirms setting for this call.
    * `:confirm_timeout` — override the confirm timeout (ms) for this call.

  Returns `:ok` or `{:error, reason}`. With confirms enabled, an unroutable
  message, a negative confirm, or a timeout is reported as an error tuple
  instead of being silently dropped.
  """
  def publish(message, options) when is_binary(message) do
    options = Enum.into(options, %{})

    rabbitmq_url = Map.fetch!(options, :rabbitmq_url)
    exchange = Map.fetch!(options, :exchange)
    routing_key = Map.fetch!(options, :routing_key)
    connection_name = Map.get(options, :publisher_connection_name)
    strategy = Application.get_env(:tackle, :publisher_strategy, :pooled)

    if strategy == :per_call or connection_name == :default do
      legacy_publish(rabbitmq_url, exchange, routing_key, message)
    else
      publisher_opts =
        options
        |> Map.take([:confirm, :confirm_timeout, :exchange_type])
        |> Enum.into([])

      Tackle.Publisher.publish(
        connection_name || @default_publisher_name,
        rabbitmq_url,
        exchange,
        routing_key,
        message,
        publisher_opts
      )
    end
  end

  # Legacy per-call path: opens a connection and re-declares the exchange per
  # message. Kept for backwards compatibility behind `publisher_strategy:
  # :per_call` and the explicit `publisher_connection_name: :default`.
  defp legacy_publish(rabbitmq_url, exchange, routing_key, message) do
    execute(rabbitmq_url, :default, fn channel ->
      Tackle.Exchange.create(channel, exchange)
      AMQP.Basic.publish(channel, exchange, routing_key, message, persistent: true)
    end)
  end

  @doc false
  def execute(rabbitmq_url, fun) do
    execute(rabbitmq_url, :default, fun)
  end

  @doc false
  def execute(rabbitmq_url, :default, fun) when is_binary(rabbitmq_url) and is_function(fun, 1) do
    Logger.debug("Connecting to '#{Tackle.DebugHelper.safe_uri(rabbitmq_url)}'")
    {:ok, connection} = Tackle.Connection.open(rabbitmq_url)
    {:ok, channel} = AMQP.Channel.open(connection)

    try do
      fun.(channel)
    after
      Tackle.Connection.close(connection)
    end
  end

  @doc false
  def execute(rabbitmq_url, connection_name, fun)
      when is_binary(rabbitmq_url) and is_function(fun, 1) do
    Logger.debug("Connecting to '#{Tackle.DebugHelper.safe_uri(rabbitmq_url)}'")
    {:ok, connection} = Tackle.Connection.open(connection_name, rabbitmq_url)
    {:ok, channel} = AMQP.Channel.open(connection)

    try do
      fun.(channel)
    after
      Tackle.Channel.close(channel)
    end
  end
end
