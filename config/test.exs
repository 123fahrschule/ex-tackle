import Config

config :logger, level: :warning

config :tackle,
  rabbitmq_host: System.get_env("RABBITMQ_HOST", "localhost"),
  rabbitmq_url:
    System.get_env("RABBITMQ_URL", "amqp://" <> System.get_env("RABBITMQ_HOST", "localhost")),
  rabbitmq_user: System.get_env("RABBITMQ_USER", "guest"),
  rabbitmq_password: System.get_env("RABBITMQ_PASSWORD", "guest")
