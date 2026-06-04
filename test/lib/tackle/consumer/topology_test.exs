defmodule Tackle.Consumer.TopologyTest do
  use ExUnit.Case
  alias Tackle.Consumer.Topology

  test "knows all the queue and exchange names generated" do
    assert topology =
             Topology.new(
               service: "my_service",
               remote_exchange: "remote_ex",
               routing_key: "route.me",
               retry: [delay: 2, limit: 10, dead_message_ttl: 50]
             )

    assert topology.remote_exchange == "remote_ex"
    assert topology.message_exchange == "my_service.route.me"
    assert topology.routing_key == "route.me"
    assert topology.queue == "my_service.route.me"
    assert topology.delay_queue == "my_service.route.me.delay.2"
    assert topology.dead_queue == "my_service.route.me.dead"
    assert topology.retry_delay == 2
    assert topology.retry_limit == 10
    assert topology.dead_message_ttl == 50
  end

  test "sets default retry options" do
    assert topology =
             Topology.new(
               service: "my_service",
               remote_exchange: "remote_ex",
               routing_key: "route.me",
               retry: true
             )

    assert topology.retry_delay == 10
    assert topology.retry_limit == 10
    assert topology.dead_message_ttl == 604_800 * 52
  end

  test "can disable retries" do
    assert topology =
             Topology.new(
               service: "my_service",
               remote_exchange: "remote_ex",
               routing_key: "route.me",
               retry: false
             )

    assert topology.retry_delay == 0
    assert topology.retry_limit == 0
    assert topology.dead_message_ttl == 604_800 * 52
  end
end
