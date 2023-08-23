defmodule Tackle.Mixfile do
  use Mix.Project

  def project do
    [
      app: :tackle,
      version: "1.0.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: preferred_cli_env()
    ]
  end

  def application do
    [
      applications: [:logger, :amqp, :public_key],
      extra_applications: [:inets],
      mod: {Tackle, []}
    ]
  end

  defp deps do
    [
      {:amqp, "~> 3.2"},
      {:ex_spec, "~> 2.0", only: :test},
      {:excoveralls, "~> 0.10", only: :test},
      {:tesla, "~> 1.4", only: :test}
    ]
  end

  defp preferred_cli_env do
    [
      coveralls: :test,
      "coveralls.detail": :test,
      "coveralls.post": :test,
      "coveralls.html": :test
    ]
  end
end
