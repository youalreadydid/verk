defmodule Verk.QueueConsumer do
  @moduledoc """
  QueueConsumer interacts with redis to dequeue jobs from the specified queue.
  """

  use GenServer
  require Logger
  alias Verk.{Queue, WorkersManager}

  @max_jobs 100

  defmodule State do
    @moduledoc false
    defstruct [:queue, :workers_manager_name, :redis, :node_id, :demand]
  end

  @doc """
  Returns the atom that represents the QueueConsumer of the `queue`
  """
  @spec name(binary | atom) :: atom
  def name(queue) do
    String.to_atom("#{queue}.queue_consumer")
  end

  def ask(queue_consumer_name, n) do
    GenServer.cast(queue_consumer_name, {:ask, n})
  end

  @doc false
  def start_link(queue_consumer_name, queue_name) do
    GenServer.start_link(__MODULE__, [queue_name], name: queue_consumer_name)
  end

  def init([queue]) do
    node_id = Confex.fetch_env!(:verk, :local_node_id)
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))

    state = %State{ queue: queue, workers_manager_name: WorkersManager.name(queue),
                    redis: redis, node_id: node_id, demand: 0 }
 # Move this to an initializer process somewhere inside the verk supervisor
    # Redix.command(redis, ["XGROUP", "CREATE", Queue.queue_name(queue), "verk", 0, "MKSTREAM"])

    Logger.info("Queue Consumer started for queue #{queue}")
    {:ok, state}
  end

  def handle_cast({:ask, new_demand}, state) do
    IO.puts "Demand received: #{new_demand}. Current demand: #{state.demand}"
    send self(), :consume
    {:noreply, %{state | demand: state.demand + new_demand }}
  end

  # FIXME infinity?
  def handle_info(:consume, state) do
    Logger.info "Consuming. Demand: #{state.demand}"

    commands = ["XREADGROUP", "GROUP", "verk", state.node_id,
                "COUNT", state.demand,
                "BLOCK", 0,
                "STREAMS", Queue.queue_name(state.queue), ">"]
    case Redix.command(state.redis, commands, timeout: :infinity) do
      {:ok, [[_, jobs]]} ->
        IO.puts "Items consumed"


        send state.workers_manager_name, {:jobs, jobs}

        new_demand = state.demand - Enum.count(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | demand: new_demand}}
      result ->
        IO.inspect result
        IO.puts "Nothing consumed"
        {:noreply, [], state}
    end
  end
end
