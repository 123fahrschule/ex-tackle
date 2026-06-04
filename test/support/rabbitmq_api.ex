defmodule Support.RabbitmqAPI do
  @user Application.compile_env(:tackle, :rabbitmq_user)
  @password Application.compile_env(:tackle, :rabbitmq_password)
  @host Application.compile_env(:tackle, :rabbitmq_host)

  @client Req.new(
            base_url: "http://#{@host}:15672/api",
            auth: {:basic, "#{@user}:#{@password}"}
          )

  def list_exchanges() do
    Req.get!(@client, url: "/exchanges")
  end

  def list_queues() do
    Req.get!(@client, url: "/queues")
  end
end
