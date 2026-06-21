# Changelog

## Unreleased

### Enhancements

* Added an `on_retries_exhausted/3` consumer callback that is invoked exactly once, after the final failed attempt when the message is routed to the dead queue. Use it to report final errors (e.g. to Sentry) instead of comparing `current_attempt` with `max_number_of_attempts` inside `on_error/5`.
* Publishing now routes through a long-lived, supervised pooled publisher (`Tackle.Publisher`) instead of opening a new connection per message. Each publisher (one per connection name) owns a persistent connection and a small channel pool (via the new `nimble_pool` dependency), and declares each exchange only once (re-declaring on reconnect). This fixes the connection churn under load described in the architecture review (Finding 4). See the README "Publishing" and "Migrating to the pooled publisher" sections.
* The retry/dead-letter path now publishes through the shared pooled publisher instead of opening a fresh connection per retried message, so error storms no longer multiply connections.
* Added optional publisher confirms, configurable globally (`config :tackle, publisher_confirms: true`) and per call (`Tackle.publish(msg, %{..., confirm: true})`), with a configurable timeout. `Tackle.publish/2` now returns `{:error, reason}` on a negative confirm, a timeout, or an unroutable message instead of silently dropping it (Finding 10). Confirms are off by default.
* Added the `publisher_strategy`, `publisher_pool_size`, `publisher_confirms`, `publisher_confirm_timeout`, and `publisher_reconnect_interval` configuration keys. See the README "Configuration reference".
* Documented `publisher_connection_name` in the README (analogous to the consumer `connection_id`).

### Fixed

* Fixed the `max_number_of_attemts` typo (now `max_number_of_attempts`) in the `Tackle.Consumer.Behaviour` callback types and documentation.
* Made `Tackle.Connection.reset/0` defensive against cached connections whose process has already died.

## v1.1.1 - 2026-06-04

### Changed

* Removed the `ex_spec` dependency.
* Migrated the test suite from ExSpec to ExUnit.

## v1.1.0 - 2026-06-04

### Changed

* Updated the supported Elixir version requirement from `~> 1.6` to `~> 1.15`.
* Updated the AMQP dependency from `~> 3.2` to `~> 4.1`.
* Updated local and CI RabbitMQ images to RabbitMQ 4.2.
* Replaced the test support HTTP client dependency from Tesla with Req.
* Removed the ExCoveralls dependency and Coveralls-specific Mix configuration.
* Modernized CI to Ubuntu 24.04, current GitHub Actions versions, Elixir 1.19.5, and Erlang/OTP 28.5.

## v1.0.1 - 2024-08-26

### Enhancements

* Added `:publisher_connection_name` to `Tackle.publish/2` options so publishers can use named, reusable AMQP connections.
* Allowed consumers to override the generated `child_spec/1`.
* Added support for secure `amqps://` connections with peer verification using OS CA certificates.
* Added production warnings when opening insecure non-`amqps://` connections.
* Added named AMQP connections for better connection visibility in RabbitMQ.
* Added automatic consumer reconnect handling when the channel process goes down.
* Added retry handling for channel setup and consumer start failures.

### Fixed

* Closed channels more consistently for publisher and consumer executor flows.
* Improved connection-cache handling when a cached connection process is no longer alive.
* Improved handling of parallel connection start requests.
* Updated deprecated Logger calls.
* Improved error logging for connection, setup, and consumption failures.

### Changed

* Updated AMQP support from the 2.x client line to the 3.x client line.
* Added `:public_key` and `:inets` to the application configuration to support TLS and test helpers.
* Replaced test helper shell calls to `rabbitmqctl` with RabbitMQ management API calls.
* Split test support helpers into dedicated support modules.
* Added a devcontainer setup, RabbitMQ configuration, and updated development tooling.

### Breaking Changes

* `Tackle.Channel.create/2` now returns `{:ok, channel}` or an error tuple. Use `Tackle.Channel.create!/2` for the previous bang-style behavior.
* `Tackle.Channel.close/1` now returns the AMQP close result directly. Use `Tackle.Channel.close!/1` when a strict `:ok` match is required.

## v1.0.0 - 2021-06-18

### Breaking Changes

* Updated the AMQP dependency from 1.x to 2.x.

## v0.2.1 - 2021-06-18

### Changed

* Added GitHub Actions CI.
* Added Dependabot configuration.
* Removed Codeship badges from the README.
* Updated locked dependencies to support Erlang/OTP 24.
* Replaced test support `rabbitmqctl` shell calls with RabbitMQ management API calls.
* Enabled the RabbitMQ management plugin for CI test runs.

## v0.2.0

### Enhancements

* Consumer knows its routing_key, call: `MyConsumer.routing_key/0`
* updated amqp which results in chatty logs, use following config to mute it

  ```elixir
  # disable amqp hex package logs
  config :lager,
    error_logger_redirect: false,
    handlers: [level: :critical]
  ```

### Breaking Changes

* Consumer process stops if connection is lost
* `use Tackle.Consumer` option `url` is renamed to `rabbitmq_url`
* `Tackle.publish` option `url` is renamed to `rabbitmq_url`
* `use Tackle.Consumer` option `exchange` is renamed to `remote_exchange`
