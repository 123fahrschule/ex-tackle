defmodule Tackle.Connection do
  use Agent
  require Logger

  @moduledoc """
  Holds established connections.
  Each connection is identifed by name.

  Connection name ':default' is special: it is NOT persisted ->
  each open() call with  :default connection name opens new connection
  (to preserve current behaviour).
  """

  def start_link, do: start_link([])

  def start_link(opts) do
    {cache, opts} = Keyword.pop(opts, :initial_value, %{})
    opts = Keyword.merge([name: __MODULE__], opts)

    Agent.start_link(fn -> cache end, opts)
  end

  # Open a new connection without caching it
  def open(url) do
    open(:default, url)
  end

  @doc """
  Open a new connection without caching it

  Examples:
      open(:default, [])

      open(:foo, [])
  """
  def open(:default, url) do
    uncached_open_connection(url, _management_interface_name = "short_lived_tackle_connection")
  end

  def open(name, url) do
    Agent.get_and_update(__MODULE__, fn connection_cache ->
      connection_cache
      |> Map.get(name)
      |> case do
        nil ->
          with {:ok, connection} <- uncached_open_connection(url, name) do
            connection_cache = Map.put(connection_cache, name, connection)
            {{:ok, connection}, connection_cache}
          else
            error ->
              {error, connection_cache}
          end

        connection ->
          if Process.alive?(connection.pid) do
            Logger.debug("Fetched existing connection #{inspect(connection)}(name: `#{name}`)")
            {{:ok, connection}, connection_cache}
          else
            connection_cache = Map.delete(connection_cache, name)

            Logger.debug(
              "Existing connection #{inspect(connection)}(name: `#{name}`) died, reconnecting..."
            )

            with {:ok, connection} <- uncached_open_connection(url, name) do
              connection_cache = Map.put(connection_cache, name, connection)
              {{:ok, connection}, connection_cache}
            else
              error ->
                {error, connection_cache}
            end
          end
      end
    end)
  end

  def close(conn) do
    AMQP.Connection.close(conn)
  end

  def reset do
    connections = get_all()
    Agent.update(__MODULE__, fn _state -> %{} end)

    Enum.each(connections, fn {_name, conn} ->
      # Be defensive: a cached connection may already be down (e.g. closed by a
      # publisher reconnect), and closing a dead connection would crash.
      if Process.alive?(conn.pid) do
        try do
          Tackle.Connection.close(conn)
        catch
          _kind, _reason -> :ok
        end
      end
    end)

    :ok
  end

  @doc """
  Get a list of opened connections
  """
  def get_all do
    Agent.get(__MODULE__, fn state -> state |> Map.to_list() end)
  end

  defp uncached_open_connection("amqps" <> _ = url, name) do
    certs = :public_key.cacerts_get()

    with {:ok, connection} <-
           AMQP.Connection.open(url,
             name: to_string(name),
             ssl_options: [
               verify: :verify_peer,
               cacerts: certs,
               customize_hostname_check: [
                 match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
               ]
             ]
           ) do
      Logger.info("Opened new secure connection #{inspect(connection)}(name: `#{name}`)")
      {:ok, connection}
    else
      error ->
        Logger.error(
          "Failed to open new secure connection(name: `#{name}`) due to `#{inspect(error)}`"
        )

        error
    end
  end

  defp uncached_open_connection(url, name) do
    if Mix.env() == :prod do
      Logger.error(
        "You are starting tackle without a secure amqps:// connection in production. This is a serious vulnerability of your system. Please specify a secure amqps:// URL."
      )
    end

    with {:ok, connection} <- AMQP.Connection.open(url, name: to_string(name)) do
      Logger.info("Opened new insecure connection #{inspect(connection)}(name: `#{name}`)")
      {:ok, connection}
    else
      error ->
        Logger.error(
          "Failed to open new insecure connection(name: `#{name}`) due to `#{inspect(error)}`"
        )

        error
    end
  end
end
