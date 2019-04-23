defmodule Verk.QueueManager do
  @moduledoc """
  QueueManager handles consumption, acknowledgment and retry of a queue
  """

  use GenServer
  require Logger
  alias Verk.{Queue, DeadSet, RetrySet, Time, Job, InProgressQueue}

  @default_stacktrace_size 5

  @max_jobs 100

  defmodule State do
    @moduledoc false
    defstruct [:queue_name, :redis, :node_id, :track_node_id]
  end

  @doc """
  Returns the atom that represents the QueueManager of the `queue`
  """
  @spec name(binary | atom) :: atom
  def name(queue) do
    String.to_atom("#{queue}.queue_manager")
  end

  @doc false
  def start_link(queue_manager_name, queue_name) do
    GenServer.start_link(__MODULE__, [queue_name], name: queue_manager_name)
  end

  @doc """
  Pop jobs from the assigned queue and reply with it if not empty
  """
  def dequeue(queue_manager, n, timeout \\ 5000) do
    GenServer.call(queue_manager, {:dequeue, n}, timeout)
  catch
    :exit, {:timeout, _} -> :timeout
  end

  @doc """
  Add job to be retried in the assigned queue
  """
  def retry(queue_manager, job, exception, stacktrace, timeout \\ 5000) do
    now = Time.now() |> DateTime.to_unix()
    GenServer.call(queue_manager, {:retry, job, now, exception, stacktrace}, timeout)
  catch
    :exit, {:timeout, _} -> :timeout
  end

  @doc """
  Acknowledge that a job was processed
  """
  def ack(queue_manager, job) do
    GenServer.cast(queue_manager, {:ack, job})
  end

  @doc """
  Remove a malformed job from the inprogress queue
  """
  def malformed(queue_manager, job) do
    GenServer.cast(queue_manager, {:malformed, job})
  end

  @doc """
  Enqueue inprogress jobs back to the queue
  """
  def enqueue_inprogress(queue_manager) do
    GenServer.call(queue_manager, :enqueue_inprogress)
  end

  @doc """
  Connect to redis
  """
  def init([queue_name]) do
    node_id = Confex.fetch_env!(:verk, :local_node_id)
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))
    Verk.Scripts.load(redis)

    state = %State{ queue_name: queue_name, redis: redis, node_id: node_id }
 # Move this to an initializer process somewhere inside the verk supervisor
    # Redix.command(redis, ["XGROUP", "CREATE", Queue.queue_name(queue_name), "verk", 0, "MKSTREAM"])

    Logger.info("Queue Manager started for queue #{queue_name}")
    {:ok, state}
  end

  @doc false
  def handle_call(:enqueue_inprogress, _from, state) do
        {:reply, :ok, state}
  end

  def handle_call({:dequeue, n}, _from, state) do
    commands = ["XREADGROUP", "GROUP", "verk", state.node_id, "COUNT", n, "STREAMS", Queue.queue_name(state.queue_name), ">"]
    case Redix.command(state.redis, commands) do
      {:ok, [[_, jobs]]} ->
        {:reply, jobs, state}

      {:ok, nil} ->
        {:reply, [], state}

      {:error, %Redix.Error{message: message}} ->
        Logger.error("Failed to fetch jobs: #{message}")
        {:stop, :redis_failed, :redis_failed, state}

      {:error, _} ->
        {:reply, :redis_failed, state}
    end
  end

  def handle_call({:retry, job, failed_at, exception, stacktrace}, _from, state) do
    retry_count = (job.retry_count || 0) + 1
    job = build_retry_job(job, retry_count, failed_at, exception, stacktrace)

    if retry_count <= (job.max_retry_count || Job.default_max_retry_count()) do
      RetrySet.add!(job, failed_at, state.redis)
    else
      Logger.info("Max retries reached to job_id #{job.jid}, job: #{inspect(job)}")
      DeadSet.add!(job, failed_at, state.redis)
    end

    {:reply, :ok, state}
  end

  defp build_retry_job(job, retry_count, failed_at, exception, stacktrace) do
    job = %{
      job
      | error_backtrace: format_stacktrace(stacktrace),
        error_message: Exception.message(exception),
        retry_count: retry_count
    }

    if retry_count > 1 do
      # Set the retried_at if this job was already retried at least once
      %{job | retried_at: failed_at}
    else
      # Set the failed_at if this the first time the job failed
      %{job | failed_at: failed_at}
    end
  end

  @doc false
  def handle_cast({:ack, item_id}, state) do
    case Queue.remove(state.queue_name, item_id) do
      {:ok, [1, 1]} -> :ok
      _ -> Logger.error("Failed to acknowledge job #{inspect(item_id)}")
    end

    {:noreply, state}
  end

  @doc false
  def handle_cast({:malformed, item_id}, state) do
    case Queue.remove(state.queue_name, item_id) do
      {:ok, 1} -> :ok
      _ -> Logger.error("Failed to remove malformed job #{inspect(item_id)}")
    end

    {:noreply, state}
  end

  defp format_stacktrace(stacktrace) when is_list(stacktrace) do
    stacktrace_limit =
      Confex.get_env(:verk, :failed_job_stacktrace_size, @default_stacktrace_size)

    Exception.format_stacktrace(Enum.slice(stacktrace, 0..(stacktrace_limit - 1)))
  end

  defp format_stacktrace(stacktrace), do: inspect(stacktrace)
end
