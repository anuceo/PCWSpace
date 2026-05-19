import Config

config :garden, GardenWeb.Endpoint,
  url: [host: "localhost"],
  render_errors: [formats: [json: GardenWeb.ErrorJSON]],
  pubsub_server: Garden.PubSub,
  live_view: [signing_salt: "maiden_tree_salt"]

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

import_config "#{config_env()}.exs"
