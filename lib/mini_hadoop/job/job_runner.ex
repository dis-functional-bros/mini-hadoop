defmodule MiniHadoop.Job.JobRunner do
  use GenServer
  require Logger

  alias MiniHadoop.Models.ComputeTask
  alias MiniHadoop.Master.ComputeOperation

  defstruct [
    :job,
    :map_tasks,
    :reduce_tasks,
    :shuffle_data,
    :status,
    :started_at,
    :participating_workers,
    :total_map_tasks,
    :total_reduce_tasks
  ]

  @ets_table :job_runner_counters

  def start_link(job) do
    GenServer.start_link(__MODULE__, job)
  end

  @impl true
  def init(job) do
    :ets.new(@ets_table, [:set, :private, :named_table])
    initialize_counters()

    state = %__MODULE__{
      job: job,
      status: :starting,
      started_at: DateTime.utc_now()
    }

    GenServer.cast(ComputeOperation, {:job_started, job.job_id})
    Process.send(self(), :start_processing, [])
    {:ok, state}
  end

  # Task Handler - detect task type from task_id prefix
  def handle_info({:task_completed, task_id}, state) do

    if String.starts_with?(task_id, "red_") do
      handle_reduce_task_completed(state)
    else
      handle_map_task_completed(state)
    end

    {:noreply, state}
  end

  def handle_info({:task_failed, task_id, error}, state) do
    Logger.error("Task failed: #{task_id}, error: #{error}")

    if String.starts_with?(task_id, "red_") do
      handle_reduce_task_failed(state)
    else
      handle_map_task_failed(state)
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, dead_worker_pid, reason}, state) do
    Logger.error("Worker died: #{inspect(dead_worker_pid)}, reason: #{reason}")
    {:noreply, state}
  end

  # Phase transition handler
  def handle_info(:start_processing, state) do
    Logger.info("Starting job processing")
    new_state = state
    |> notify_job_start()
    |> execute_map_phase()
    {:noreply, new_state}
  end

  def handle_info(:proceed_to_shuffle, state) do
    Logger.info("Proceeding to shuffle phase")
    {:noreply, execute_shuffle_phase(state)}
  end

  def handle_info(:proceed_to_completion, state) do
    Logger.info("Job #{state.job.job_id}: Proceeding to completion")
    final_state = notify_job_completion(state)
    :ets.delete(@ets_table)
    {:stop, :normal, final_state}
  end

  def handle_info({:job_failed, error}, state) do
    Logger.error("Job #{state.job.job_id} failed: #{inspect(error)}")
    final_state = notify_job_failure(state, error)
    :ets.delete(@ets_table)
    {:stop, :normal, final_state}
  end

  def handle_info(:start_processing, state) do
    Logger.info("Starting job processing")
    new_state = state
    |> notify_job_start()
    |> execute_map_phase()
    {:noreply, new_state}
  end

  def handle_cast({:update_participating_worker, worker_pid}, state) do
    updated_workers = MapSet.put(state.participating_workers || MapSet.new(), worker_pid)
    {:noreply, %{state | participating_workers: updated_workers}}
  end

  # Simple counter handlers
  defp handle_map_task_completed(state) do
    completed = :ets.update_counter(@ets_table, :completed_map_tasks, 1)
    update_progress(:map, completed, get_failed_map_tasks(), state.total_map_tasks, state.job.job_id)
    check_map_completion(state, completed, get_failed_map_tasks())
  end

  defp handle_reduce_task_completed(state) do
    completed = :ets.update_counter(@ets_table, :completed_reduce_tasks, 1)
    update_progress(:reduce, completed, get_failed_reduce_tasks(), state.total_reduce_tasks, state.job.job_id)
    check_reduce_completion(state, completed, get_failed_reduce_tasks())
  end

  defp handle_map_task_failed(state) do
    failed = :ets.update_counter(@ets_table, :failed_map_tasks, 1)
    update_progress(:map, get_completed_map_tasks(), failed, state.total_map_tasks, state.job.job_id)
    check_map_completion(state, get_completed_map_tasks(), failed)
  end

  defp handle_reduce_task_failed(state) do
    failed = :ets.update_counter(@ets_table, :failed_reduce_tasks, 1)
    update_progress(:reduce, get_completed_reduce_tasks(), failed, state.total_reduce_tasks, state.job.job_id)
    check_reduce_completion(state, get_completed_reduce_tasks(), failed)
  end

  # Completion checks
  defp check_map_completion(state, completed, failed) do
    if completed + failed >= state.total_map_tasks do
      Logger.info("All map tasks completed (completed: #{completed}, failed: #{failed})")
      Process.send(self(), :proceed_to_shuffle, [])
    end
  end

  defp check_reduce_completion(state, completed, failed) do
    if completed + failed >= state.total_reduce_tasks do
      Logger.info("All reduce tasks completed (completed: #{completed}, failed: #{failed})")
      Process.send(self(), :proceed_to_completion, [])
    end
  end

  # Progress updates
  defp update_progress(phase, completed, failed, total, job_id) do
    GenServer.cast(ComputeOperation, {
      :job_progress,
      job_id,
      phase,
      completed + failed,
      total
    })
  end

  # Counter helpers
  defp initialize_counters do
    :ets.insert(@ets_table, [
      {:completed_map_tasks, 0},
      {:completed_reduce_tasks, 0},
      {:failed_map_tasks, 0},
      {:failed_reduce_tasks, 0}
    ])
  end

  defp get_completed_map_tasks, do: get_counter(:completed_map_tasks)
  defp get_completed_reduce_tasks, do: get_counter(:completed_reduce_tasks)
  defp get_failed_map_tasks, do: get_counter(:failed_map_tasks)
  defp get_failed_reduce_tasks, do: get_counter(:failed_reduce_tasks)

  defp get_counter(key) do
    [{^key, value}] = :ets.lookup(@ets_table, key)
    value
  end

  # Job execution pipeline
  defp execute_map_phase(state) do
    try do
      state
      |> notify_phase_start(:map)
      |> generate_map_tasks()
      |> dispatch_map_tasks()
      |> wait_for_map_completion()
    rescue
      error ->
        Process.send(self(), {:job_failed, error}, [])
        state
    end
  end

  defp execute_reduce_phase(state) do
    try do
      state
      |> notify_phase_start(:reduce)
      |> generate_reduce_tasks()
      |> dispatch_reduce_tasks()
      |> wait_for_reduce_completion()
    rescue
      error ->
        Process.send(self(), {:job_failed, error}, [])
        state
    end
  end

  defp execute_shuffle_phase(state) do
    Logger.info("Executing shuffle phase")

    # Reset reduce phase counters
    :ets.insert(@ets_table, [
      {:completed_reduce_tasks, 0},
      {:failed_reduce_tasks, 0}
    ])


    # fetch result map, it will look like this:
    #
    # TODO
    # %{
    #   worker_pid_1 =>#MapSet<["key1", "key3", "key2"]>,
    #   worker_pid_2 =>#MapSet<["key1", "key3", "key2"]>,
    #   worker_pid_3 =>#MapSet<["key1", "key3", "key2"]>
    # }
    #
    # Output: [{"key1", [worker_pid_1, worker_pid_2, worker_pid_3]}, {"key", [worker_pid_1, worker_pid_2, worker_pid_3]}, {"key", [worker_pid_1, worker_pid_2, worker_pid_3]}]

    # Generate dummy shuffle data
    workers_pids = ComputeOperation.get_workers()
    shuffle_result = [{"dummy", Enum.take(workers_pids, 2)}, {"data", [Enum.at(workers_pids, 0)]}]

    %{state | shuffle_data: shuffle_result, status: :shuffle_completed}
    |> execute_reduce_phase()
  end

  # Phase operations
  defp notify_phase_start(state, phase) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.job_id, phase})
    state
  end

  defp generate_map_tasks(state) do
    map_tasks = generate_map_tasks_for_job(state.job)
    Logger.info("Generated #{length(map_tasks)} map tasks")
    %{state | map_tasks: map_tasks}
  end

  defp generate_reduce_tasks(state) do
    reduce_tasks = generate_reduce_tasks_for_job(state.job, state.shuffle_data)
    Logger.info("Generated #{length(reduce_tasks)} reduce tasks")
    %{state | reduce_tasks: reduce_tasks}
  end

  defp dispatch_map_tasks(state) do
    {dispatched, failed, _assignments} = dispatch_tasks(state.map_tasks, {0, 0, %{}})
    Logger.info("Dispatched #{dispatched} map tasks, #{failed} failed")

    %{state |
      map_tasks: nil,
      total_map_tasks: dispatched
    }
  end

  defp dispatch_reduce_tasks(state) do
    {dispatched, failed, _assignments} = dispatch_tasks(state.reduce_tasks, {0, 0, %{}})
    Logger.info("Dispatched #{dispatched} reduce tasks, #{failed} failed")

    %{state |
      reduce_tasks: nil,
      total_reduce_tasks: dispatched
    }
  end

  defp wait_for_map_completion(state) do
    if state.total_map_tasks > 0 do
      %{state | status: :map_waiting}
    else
      Process.send(self(), :proceed_to_shuffle, [])
      state
    end
  end

  defp wait_for_reduce_completion(state) do
    if state.total_reduce_tasks > 0 do
      %{state | status: :reduce_waiting}
    else
      Process.send(self(), :proceed_to_completion, [])
      state
    end
  end

  # Notification functions
  defp notify_job_start(state) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.job_id, :starting})
    state
  end

  defp notify_job_completion(state) do
    results = generate_final_results(state)
    GenServer.cast(ComputeOperation, {:job_completed, state.job.job_id, results})
    %{state | status: :completed}
  end

  defp notify_job_failure(state, error) do
    GenServer.cast(ComputeOperation, {:job_failed, state.job.job_id, error})
    %{state | status: :failed}
  end

  # Task generation and dispatch (simplified - no assignment tracking)
  defp generate_map_tasks_for_job(job) do
    worker_pids = ComputeOperation.get_workers()
    # TODO real task generation

    1..10
    |> Enum.map(fn task_id ->
      worker_index = rem(task_id - 1, length(worker_pids))
      worker_pid = Enum.at(worker_pids, worker_index)
      block_info = {"block_#{task_id}", [worker_pid]}
      ComputeTask.new_map(job.job_id, block_info, job.map_function, self())
    end)
  end

  defp generate_reduce_tasks_for_job(job, shuffle_data) do
    # TODO real task generation
    # %{key, key, key}

    shuffle_data
    |> Enum.map(fn {key, worker_pids} ->
      ComputeTask.new_reduce(
        job.job_id,
        [{key, worker_pids}],
        job.reduce_function,
        self()
      )
    end)
  end

  defp dispatch_tasks([], acc), do: acc

  defp dispatch_tasks([task | remaining_tasks], {dispatched, failed, _mapping}) do
    case execute_task(task) do
      {:ok, worker_pid} ->
        GenServer.cast(self(), {:update_participating_worker, worker_pid})
        dispatch_tasks(remaining_tasks, {dispatched + 1, failed, %{}})

      {:ok, :all_workers_queue_full} ->
        Logger.debug("All workers queue are full, retrying")
        dispatch_tasks([task | remaining_tasks], {dispatched, failed, %{}})

      {:error, reason} ->
        Logger.error("Failed to dispatch task #{task.task_id}: #{reason}")
        dispatch_tasks(remaining_tasks, {dispatched, failed + 1, %{}})

      {:error, :no_worker} ->
        remaining_count = length(remaining_tasks)
        dispatch_tasks([], {dispatched, failed + 1 + remaining_count, %{}})
    end
  end

  defp generate_final_results(state) do
    %{
      total_map_tasks: state.total_map_tasks,
      total_reduce_tasks: state.total_reduce_tasks,
      output_files: [
        "/placeholder/result/#{state.job.job_id}.txt",
      ],
    }
  end

  defp execute_task(task) do
    choose_worker(task)
    |> then(fn
      {:ok, worker_pid} ->
        GenServer.cast(worker_pid, {:execute_task, task})
        {:ok, worker_pid}

      {:error, reason} ->
        {:error, reason}
    end)
  end

  defp choose_worker(%{type: :map, input_data: {_block_id, [worker_pid | _]}}) do
    {:ok, worker_pid}
  end

  defp choose_worker(%{type: :reduce}) do
    available_workers = ComputeOperation.get_workers()

    case available_workers do
      [] -> {:error, :no_worker}
      workers ->
        worker = Enum.random(workers)
        {:ok, worker}
    end
  end

  defp choose_worker(_task) do
    {:error, "Invalid task structure"}
  end
end
