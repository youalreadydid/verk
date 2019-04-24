defmodule Verk.NodeTest do
  use ExUnit.Case, async: true
  import Verk.Node
  doctest Verk.Node

  @verk_nodes_key "verk_nodes"

  @node "123"
  @node_key "verk:node:123"
  @node_queues_key "verk:node:123:queues"

  setup do
    {:ok, redis} = :verk |> Confex.get_env(:redis_url) |> Redix.start_link()
    Redix.command!(redis, ["DEL", @verk_nodes_key, @node_queues_key])
    {:ok, %{redis: redis}}
  end

  defp register(%{redis: redis}) do
    register(redis, @node)
    :ok
  end

  defp register(redis, verk_node) do
    expire_in(verk_node, 555, redis)
  end

  describe "ttl!/2" do
    setup :register

    test "return ttl from a verk node id", %{redis: redis} do
      assert_in_delta ttl!(@node, redis), 555, 5
    end
  end

  describe "expire_in/3" do
    test "resets expiration item", %{redis: redis} do
      assert {:ok, _} = expire_in(@node, 888, redis)
      assert_in_delta Redix.command!(redis, ["PTTL", @node_key]), 888, 5
    end
  end
end
