defmodule RedisScriptsTest do
  use ExUnit.Case, async: true

  @enqueue_retriable_job_script File.read!("#{:code.priv_dir(:verk)}/enqueue_retriable_job.lua")
  @requeue_job_now_script File.read!("#{:code.priv_dir(:verk)}/requeue_job_now.lua")

  setup do
    {:ok, redis} = Confex.get_env(:verk, :redis_url) |> Redix.start_link()
    {:ok, %{redis: redis}}
  end

  describe "enqueue_retriable_job" do
    test "enqueue job to queue form retry set", %{redis: redis} do
      job = "{\"jid\":\"123\",\"enqueued_at\":\"42\",\"queue\":\"test_queue\"}"
      other_job = "{\"jid\":\"456\",\"queue\":\"test_queue\"}"
      enqueued_job = "{\"jid\":\"789\",\"queue\":\"test_queue\"}"

      {:ok, _} = Redix.command(redis, ~w(DEL retry queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD retry 42 #{job} 45 #{other_job}))
      {:ok, _} = Redix.command(redis, ~w(LPUSH queue:test_queue #{enqueued_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "41"]) ==
               {:ok, nil}

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "42"]) ==
               {:ok, job}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) ==
               {:ok, [other_job, "45"]}

      assert Redix.command(redis, ~w(LRANGE queue:test_queue 0 -1)) == {:ok, [job, enqueued_job]}
    end

    test "enqueue job to queue form schedule set", %{redis: redis} do
      scheduled_job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      enqueued_scheduled_job = "{\"jid\":\"123\",\"enqueued_at\":42,\"queue\":\"test_queue\"}"

      {:ok, _} = Redix.command(redis, ~w(DEL schedule queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD schedule 42 #{scheduled_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "41"]) ==
               {:ok, nil}

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "42"]) ==
               {:ok, enqueued_scheduled_job}
    end

    test "enqueue job to queue form null enqueued_at key", %{redis: redis} do
      scheduled_job = "{\"jid\":\"123\",\"enqueued_at\":null,\"queue\":\"test_queue\"}"
      enqueued_scheduled_job = "{\"jid\":\"123\",\"enqueued_at\":42,\"queue\":\"test_queue\"}"

      {:ok, _} = Redix.command(redis, ~w(DEL schedule queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD schedule 42 #{scheduled_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "41"]) ==
               {:ok, nil}

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "42"]) ==
               {:ok, enqueued_scheduled_job}
    end
  end

  describe "requeue_job_now" do
    test "improper job format returns job data doesn't move job", %{redis: redis} do
      job_with_no_queue = "{\"jid\":\"123\"}"
      {:ok, _} = Redix.command(redis, ~w(DEL retry queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD retry 42 #{job_with_no_queue}))

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job_with_no_queue]) ==
               {:ok, job_with_no_queue}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) ==
               {:ok, [job_with_no_queue, "42"]}
    end

    test "valid job format, requeue job moves from retry to original queue", %{redis: redis} do
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      {:ok, _} = Redix.command(redis, ~w(DEL retry queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD retry 42 #{job}))

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) ==
               {:ok, [job, "42"]}

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job]) ==
               {:ok, job}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == {:ok, []}
    end

    test "valid job format, empty original queue job is still requeued", %{redis: redis} do
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      {:ok, _} = Redix.command(redis, ~w(DEL retry queue:test_queue))

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == {:ok, []}

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job]) ==
               {:ok, job}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == {:ok, []}
    end
  end
end
