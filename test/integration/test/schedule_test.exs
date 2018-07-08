defmodule IntegrationTest.Schedule do
  use ExUnit.Case
  alias Verk.Events.{JobStarted, JobFinished}

  defmodule Consumer do
    use GenStage

    def start(), do: GenStage.start(__MODULE__, self())

    def init(parent) do

      filter = fn event -> event.__struct__ in [JobStarted, JobFinished] end
      {:consumer, parent, subscribe_to: [{Verk.EventProducer, selector: filter}]}
    end

    def handle_events(events, _from, parent) do
      Enum.each(events, fn event -> send parent, event end)
      {:noreply, [], parent}
    end
  end

  setup_all do
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))
    Redix.command!(redis, ["FLUSHDB"])
    {:ok, redis: redis}
  end

  @tag integration: true
  test "schedule", %{redis: redis} do
    perform_at = DateTime.utc_now
    arg = 393_087_288_629_462_191
    Verk.schedule(%Verk.Job{queue: :queue_one, class: Integration.EqualWorker, args: [arg, to_string(arg)]}, perform_at, redis)
    Application.ensure_all_started(:integration)
    {:ok, _consumer} = Consumer.start
    :timer.sleep 10_000
    Application.stop(:integration)

    assert_receive %Verk.Events.JobStarted{}, 1_000
    assert_receive %Verk.Events.JobFinished{}, 1_000
  end
end
