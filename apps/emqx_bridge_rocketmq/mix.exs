defmodule EMQXBridgeRocketmq.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_bridge_rocketmq,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: EMQXUmbrella.MixProject.erlc_options(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: []]
  end

  def deps() do
    [
      {:rocketmq, github: "emqx/rocketmq-client-erl", tag: "v0.6.1"},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end