defmodule MiniHadoop.Job.JobRunner do
  use GenServer
  require Logger

  alias MiniHadoop.Models.ComputeTask
  alias MiniHadoop.Master.ComputeOperation

  defstruct [
    :job,
    :map_tasks,
    :reduce_tasks,
    :map_results,
    :status,
    :started_at
  ]

  # Start via DynamicSupervisor
  def start_link(job) do
    GenServer.start_link(__MODULE__, job)
  end

  @impl true
  def init(job) do
    state = %__MODULE__{
      job: job,
      status: :starting,
      started_at: DateTime.utc_now()
    }

    # Notify ComputeOperation
    GenServer.cast(ComputeOperation, {:job_started, job.job_id})

    # Start processing asynchronously
    Process.send(self(), :start_processing, [])

    {:ok, state}
  end

  def handle_info(:start_processing, state) do
    Logger.info("Job #{state.job.job_id}: Starting processing")

    state
    |> execute_job_pipeline()
    |> handle_job_result()
  end

  # Functional job execution pipeline
  defp execute_job_pipeline(state) do
    state
    |> notify_job_start()
    |> execute_map_phase()
    # |> execute_shuffle_phase()
    |> execute_reduce_phase()
    |> notify_job_completion()
  rescue
    error ->
      %{state | status: :failed}
      |> notify_job_failure(error)
      |> then(fn failed_state -> {:error, failed_state, error} end)
  end

  defp handle_job_result({:error, state, error}) do
    Logger.error("Job #{state.job.job_id} failed: #{inspect(error)}")
    {:stop, :normal, state}
  end

  defp handle_job_result(state) do
    Logger.info("Job #{state.job.job_id}: Completed, shutting down")
    {:stop, :normal, state}
  end

  # Pure state transformations with side effects at boundaries
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
    state
  end

  # Map phase as pure data transformation
  defp execute_map_phase(state) do
    state
    |> notify_map_phase_start()
    |> generate_map_tasks()
    |> dispatch_map_tasks()
    |> wait_for_map_completion()
    |> then(fn new_state -> %{new_state | status: :map_completed} end)
  end

  defp notify_map_phase_start(state) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.job_id, :map_phase})
    state
  end

  defp generate_map_tasks(state) do
    map_tasks = generate_map_tasks_for_job(state.job)
    Logger.info("Generated #{length(map_tasks)} map tasks")
    %{state | map_tasks: map_tasks}
  end

  defp dispatch_map_tasks(state) do
    {dispatched, failed, task_assignments} =
      state.map_tasks
      |> dispatch_tasks({0, 0, %{}})

    # Monitor workers for fault tolerance
    task_assignments
    |> Map.values()
    |> Enum.each(&Process.monitor/1)

    Logger.info("Dispatched #{dispatched} map tasks, #{failed} failed")

    %{state | map_tasks: nil}  # Clear tasks after dispatch
    |> Map.put(:task_assignments, task_assignments)
    |> Map.put(:total_map_tasks, dispatched)
  end

  defp wait_for_map_completion(state) do
    if state.total_map_tasks > 0 do
      wait_for_tasks_completion(
        state.total_map_tasks,
        0,
        state.task_assignments,
        state.job.job_id
      )
    end

    %{state | task_assignments: nil}  # Clean up assignments
  end

  # Reduce phase as pure data transformation
  defp execute_reduce_phase(state) do
    state
    |> notify_reduce_phase_start()
    |> execute_reduce_tasks()
    |> then(fn new_state -> %{new_state | status: :reduce_completed} end)
  end

  defp notify_reduce_phase_start(state) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.job_id, :reduce_phase})
    state
  end

  defp execute_reduce_tasks(state) do
    total_reduce_tasks = 3
    Logger.info("Starting #{total_reduce_tasks} reduce tasks")

    1..total_reduce_tasks
    |> Enum.reduce(state, fn task_num, acc ->
      Process.sleep(3000)  # Simulate work
      Logger.info("Reduce task #{task_num}/#{total_reduce_tasks} completed")

      # Update progress
      GenServer.cast(ComputeOperation, {:job_progress, acc.job.job_id, :reduce, task_num, total_reduce_tasks})
      acc
    end)
  end

  # Functional task dispatch
  defp dispatch_tasks([], acc), do: acc

  defp dispatch_tasks([task | remaining_tasks], {dispatched, failed, assignments}) do
    case execute_task(task) do
      {:ok, worker_pid} ->
        Logger.debug("Task #{task.task_id} dispatched to worker #{inspect(worker_pid)}")
        new_assignments = Map.put(assignments, task.task_id, worker_pid)
        dispatch_tasks(remaining_tasks, {dispatched + 1, failed, new_assignments})

      {:ok, :all_workers_queue_full} ->
        Logger.debug("All workers queue are full, retrying")
        dispatch_tasks([task | remaining_tasks], {dispatched, failed, assignments})

      {:error, reason} ->
        Logger.error("Failed to dispatch task #{task.task_id}: #{reason}")
        dispatch_tasks(remaining_tasks, {dispatched, failed + 1, assignments})

      {:error, :no_worker} ->
        remaining_count = length(remaining_tasks)
        dispatch_tasks([], {dispatched, failed + 1 + remaining_count, assignments})
    end
  end

  # Functional task waiting with pattern matching
  defp wait_for_tasks_completion(total_tasks, total_tasks, _assignments, _job_id), do: :ok

  defp wait_for_tasks_completion(total_tasks, received, assignments, job_id) do
    receive do
      message ->
        handle_task_message(message, total_tasks, received, assignments, job_id)
    end
  end

  defp handle_task_message({:task_completed, task_id}, total_tasks, received, assignments, job_id) do
    new_received = received + 1
    Logger.debug("Task #{task_id} completed (#{new_received}/#{total_tasks})")

    GenServer.cast(ComputeOperation, {:job_progress, job_id, :map, new_received, total_tasks})

    assignments
    |> Map.delete(task_id)
    |> then(fn new_assignments ->
      wait_for_tasks_completion(total_tasks, new_received, new_assignments, job_id)
    end)
  end

  defp handle_task_message({:task_failed, task_id, error}, total_tasks, received, assignments, job_id) do
    new_received = received + 1
    Logger.error("Task #{task_id} failed: #{error} (#{new_received}/#{total_tasks})")

    GenServer.cast(ComputeOperation, {:job_progress, job_id, :map, new_received, total_tasks})

    assignments
    |> Map.delete(task_id)
    |> then(fn new_assignments ->
      wait_for_tasks_completion(total_tasks, new_received, new_assignments, job_id)
    end)
  end

  defp handle_task_message({:DOWN, _ref, :process, dead_worker_pid, reason}, total_tasks, received, assignments, job_id) do
    Logger.error("Worker #{inspect(dead_worker_pid)} died: #{reason}")

    {failed_tasks, remaining_assignments} =
      assignments
      |> Map.split(fn {_task_id, worker_pid} -> worker_pid == dead_worker_pid end)

    tasks_failed = map_size(failed_tasks)
    new_received = received + tasks_failed

    if tasks_failed > 0 do
      Logger.error("Marking #{tasks_failed} tasks from dead worker as failed")
      GenServer.cast(ComputeOperation, {:job_progress, job_id, :map, new_received, total_tasks})

      failed_tasks
      |> Map.keys()
      |> Enum.each(fn task_id ->
        Logger.error("Task #{task_id} failed because worker died: #{reason}")
      end)
    end

    wait_for_tasks_completion(total_tasks, new_received, remaining_assignments, job_id)
  end

  defp handle_task_message(unexpected, total_tasks, received, assignments, job_id) do
    Logger.warn("Unexpected message: #{inspect(unexpected)}")
    wait_for_tasks_completion(total_tasks, received, assignments, job_id)
  end

  # Pure data generation functions
  defp generate_map_tasks_for_job(job) do
    worker_pids = ComputeOperation.get_workers()

    1..10
    |> Enum.map(fn task_id ->
      worker_index = rem(task_id - 1, length(worker_pids))
      worker_pid = Enum.at(worker_pids, worker_index)
      block_info = {"block_#{task_id}", [worker_pid]}
      ComputeTask.new_map(job.job_id, block_info, job.map_function, self())
    end)
  end

  defp generate_final_results(state) do
    %{
      total_map_tasks: 5,
      total_reduce_tasks: 3,
      output_files: [
        "/output/#{state.job.job_id}/part-00000",
        "/output/#{state.job.job_id}/part-00001"
      ],
      summary: %{
        total_records_processed: 5000,
        unique_keys: 150,
        data_size_bytes: 250000
      }
    }
  end

  # Task execution as pure function
  @spec execute_task(ComputeTask.t()) :: {:ok, pid()} | {:error, String.t()}
  defp execute_task(task) do
    task.input_data
    |> choose_worker()
    |> then(fn
      {:ok, worker_pid} ->
        GenServer.cast(worker_pid, {:execute_task, task})
        {:ok, worker_pid}

      {:error, reason} ->
        {:error, reason}
    end)
  end

  @spec choose_worker(tuple()) :: {:ok, pid()} | {:error, String.t()}
  defp choose_worker({_block_id, [worker_pid | _]}) do
    {:ok, worker_pid}
  end

  defp choose_worker(_) do
    {:error, "Invalid block info structure"}
  end
end
