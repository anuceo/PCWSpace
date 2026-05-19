defmodule Garden.MixProject do
  use Mix.Project

  def project do
    [
      app: :garden,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Garden.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:phoenix, "~> 1.7"},
      {:phoenix_pubsub, "~> 2.1"},
      {:phoenix_live_view, "~> 0.20"},
      {:jason, "~> 1.4"},
      {:grpc, "~> 0.7"},
      {:protobuf, "~> 0.12"},
      {:nimble_csv, "~> 1.2"},
      {:cloak, "~> 1.1"},
      {:libcluster, "~> 3.3"},
      {:quantum, "~> 3.5"}
    ]
  end
end
