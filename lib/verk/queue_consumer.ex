defmodule Verk.QueueConsumer do
  @moduledoc """
  QueueConsumer interacts with redis to dequeue jobs from the specified queue.
  """

  use GenServer
  require Logger
  alias Verk.{Queue, WorkersManager}

  @max_jobs 100
  @max_timeout 30_000

  defmodule State do
    @moduledoc false
    defstruct [:queue, :workers_manager_name, :redis, :node_id, :demand, :last_id]
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

    state = %State{
      queue: queue,
      workers_manager_name: WorkersManager.name(queue),
      redis: redis,
      node_id: node_id,
      demand: 0,
      last_id: 0
    }

    ensure_group_exists!(queue, redis)

    Logger.info("Queue Consumer started for queue #{queue}")
    {:ok, state}
  end

  defp ensure_group_exists!(queue, redis) do
    Redix.command(redis, ["XGROUP", "CREATE", Queue.queue_name(queue), "verk", 0, "MKSTREAM"])
  rescue
      _ -> :ok
  end

  def handle_cast({:ask, new_demand}, state) do
    IO.puts("Demand received: #{new_demand}. Current demand: #{state.demand}")
    if state.demand == 0, do: send(self(), :consume)
    {:noreply, %{state | demand: state.demand + new_demand}}
  end

  def handle_info(:consume, state = %State{last_id: ">"}) do
    case consume(state) do
      {:ok, nil} ->
        send(self(), :consume)
        {:noreply, state}

      {:ok, jobs} ->
        IO.puts("Items consumed. Size: #{length(jobs)}")

        send(state.workers_manager_name, {:jobs, jobs})

        new_demand = state.demand - Enum.count(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | demand: new_demand}}

      result ->
        IO.inspect(result)
        IO.puts("Nothing consumed")
        send(self(), :consume)
        {:noreply, [], state}
    end
  end

  def handle_info(:consume, state) do
    case consume(state) do
      {:ok, nil} ->
        send(self(), :consume)
        {:noreply, state}

      {:ok, []} ->
        send(self(), :consume)
        {:noreply, %{state | last_id: ">"}}

      {:ok, jobs} ->
        IO.puts("Items consumed. Size: #{length(jobs)}")

        send(state.workers_manager_name, {:jobs, jobs})

        [last_id, _] = List.last(jobs)

        new_demand = state.demand - Enum.count(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | last_id: last_id, demand: new_demand}}

      result ->
        IO.puts("Nothing consumed. Result: #{inspect(result)}")
        send(self(), :consume)
        {:noreply, [], state}
    end
  end

  defp consume(state) do
    Logger.info("Consuming. Demand: #{state.demand}. Last id: #{state.last_id}")

    commands = [
      "XREADGROUP",
      "GROUP",
      "verk",
      state.node_id,
      "COUNT",
      min(@max_jobs, state.demand),
      "BLOCK",
      @max_timeout,
      "STREAMS",
      Queue.queue_name(state.queue),
      state.last_id
    ]

    case Redix.command(state.redis, commands, timeout: @max_timeout + 5000) do
      {:ok, [[_, jobs]]} -> {:ok, jobs}
      result -> result |> IO.inspect
    end
  end
end
