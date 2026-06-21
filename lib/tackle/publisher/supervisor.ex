defmodule Tackle.Publisher.Supervisor do
  @moduledoc """
  Dynamic supervisor for `Tackle.Publisher` processes.

  One publisher is started per connection name, lazily on first publish, so the
  application boots even when the broker is unavailable.
  """

  use DynamicSupervisor

  def start_link(init_arg \\ []) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Ensure a publisher for `name` is running and return its pid.

  Idempotent: concurrent callers converge on the same publisher process.
  """
  def ensure_started(name, rabbitmq_url) do
    spec = {Tackle.Publisher, {name, rabbitmq_url}}

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, _reason} = error -> error
    end
  end
end
