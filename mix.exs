defmodule Tackle.Mixfile do
  use Mix.Project

  def project do
    [
      app: :tackle,
      version: "1.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_paths: ["test"]
    ]
  end

  def application do
    [extra_applications: [:logger, :public_key, :inets], mod: {Tackle, []}]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:amqp, "~> 4.1"},
      {:ex_spec, "~> 2.0", only: [:test, :dev]},
      {:req, "~> 0.5.18", only: [:test, :dev]}
    ]
  end
end
