defmodule Verk.Node do
  @moduledoc """
  Node data controller
  """

  @spec ttl!(String.t(), GenServer.t()) :: integer
  def ttl!(verk_node_id, redis) do
    Redix.command!(redis, ["PTTL", verk_node_key(verk_node_id)])
  end

  @spec expire_in(String.t(), integer, GenServer.t()) :: {:ok, integer} | {:error, term}
  def expire_in(verk_node_id, ttl, redis) do
    Redix.command(redis, ["PSETEX", verk_node_key(verk_node_id), ttl, "alive"])
  end

  defp verk_node_key(verk_node_id), do: "verk:node:#{verk_node_id}"
end
