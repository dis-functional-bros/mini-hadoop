defmodule MiniHadoop.ShuffleTest do
  use ExUnit.Case

  alias MiniHadoop.Job.JobRunner
  alias MiniHadoop.ComputeTask.TaskRunner

  # Mock ComputeOperation to avoid full system startup
  defmodule ComputeOperationMock do
    use GenServer

    def start_link(workers \\ []) do
      GenServer.start_link(__MODULE__, workers, name: MiniHadoop.Master.ComputeOperation)
    end

    def init(workers) do
      {:ok, %{workers: workers}}
    end

    def handle_cast({:job_status, _, _}, state), do: {:noreply, state}
    def handle_cast({:job_started, _}, state), do: {:noreply, state}
    def handle_cast({:job_progress, _, _, _, _}, state), do: {:noreply, state}
    def handle_call(:get_workers, _from, state), do: {:reply, state.workers, state}

    # Public API required by JobRunner
    def get_workers do
      GenServer.call(MiniHadoop.Master.ComputeOperation, :get_workers)
    end
  end

  setup do
    :ok
  end

  test "JobRunner aggregates keys correctly in shuffle phase using get_intermediate_data" do
    test_pid = self()

    # Spawn dummy workers that respond to get_intermediate_data
    worker1 = spawn(fn -> worker_loop(test_pid, ["key1", "key2"]) end)
    worker2 = spawn(fn -> worker_loop(test_pid, ["key2", "key3"]) end)

    start_supervised!({ComputeOperationMock, [worker1, worker2]})

    job = %{
      job_id: "job_shuffle_test",
      map_function: nil,
      reduce_function: nil
    }

    {:ok, runner} = JobRunner.start_link(job)

    # Trigger shuffle phase manually by simulating map completion
    :sys.replace_state(runner, fn state -> %{state | total_map_tasks: 0} end)
    send(runner, {:task_completed, "map_dummy"})

    # Wait a bit
    Process.sleep(200)

    state = :sys.get_state(runner)

    # Verify shuffle_data
    # Status might have moved to :reduce_waiting or :reduce_phase
    assert state.status != :map_phase
    assert state.shuffle_data != nil

    data = state.shuffle_data |> Map.new()

    assert Map.has_key?(data, "key1")
    assert Map.has_key?(data, "key2")
    assert Map.has_key?(data, "key3")

    assert worker1 in data["key1"]
    assert worker1 in data["key2"]
    assert worker2 in data["key2"]
    assert worker2 in data["key3"]
  end

  defp worker_loop(test_pid, keys) do
    receive do
      {:"$gen_call", from, {:get_intermediate_data, _job_id}} ->
        GenServer.reply(from, {:ok, keys})
        worker_loop(test_pid, keys)
      _ ->
        worker_loop(test_pid, keys)
    end
  end
end
