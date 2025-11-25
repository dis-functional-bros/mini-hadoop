defmodule MiniHadoop.ComputeTask.TaskRunner do
  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    {:ok, %{
      max_concurrent_tasks: opts[:max_concurrent_compute_tasks] || 1,
      running_tasks: %{},      # %{task_ref => {task, start_time}}
      pending_tasks: :queue.new(),
      completed_tasks: [],     # Track recently completed tasks
      failed_tasks: []         # Track recently failed tasks
    }}
  end

  # Public API
  def execute_task(task) do
    GenServer.cast(__MODULE__, {:execute_task, task})
  end

  def execute_task_sync(task) do
    GenServer.call(__MODULE__, {:execute_task_sync, task})
  end

  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  def cancel_task(task_id) do
    GenServer.cast(__MODULE__, {:cancel_task, task_id})
  end

  def pause_tasks do
    GenServer.cast(__MODULE__, :pause_tasks)
  end

  def resume_tasks do
    GenServer.cast(__MODULE__, :resume_tasks)
  end

  # GenServer Handlers
  def handle_call({:execute_task_sync, task}, _from, state) do
    # Execute task synchronously (blocks until completion)
    result = execute_task_logic(task)
    {:reply, {:ok, result}, state}
  end

  def handle_call(:get_status, _from, state) do
    status = %{
      running: map_size(state.running_tasks),
      pending: :queue.len(state.pending_tasks),
      completed: length(state.completed_tasks),
      failed: length(state.failed_tasks),
      max_concurrent: state.max_concurrent_tasks
    }
    {:reply, status, state}
  end

  def handle_cast({:execute_task, task}, state) do
    if can_accept_more_tasks?(state) do
      # Execute immediately
      task_ref =  Task.Supervisor.async_nolink(MiniHadoop.ComputeTask.TaskSupervisor, fn -> execute_task_logic(task) end)
      running_tasks = Map.put(state.running_tasks, task_ref, {task, DateTime.utc_now()})
      {:noreply, %{state | running_tasks: running_tasks}}
    else
      # Add to pending queue
      pending_tasks = :queue.in(task, state.pending_tasks)
      Logger.info("Task #{task.task_id} queued. #{:queue.len(pending_tasks)} tasks pending")
      {:noreply, %{state | pending_tasks: pending_tasks}}
    end
  end

  def handle_cast({:cancel_task, task_id}, state) do
    # Find task in running tasks
    case Enum.find(state.running_tasks, fn {_ref, {task, _time}} -> task.task_id == task_id end) do
      {task_ref, {task, _start_time}} ->
        # Kill the task process
        Task.Supervisor.terminate_child(MiniHadoop.ComputeTask.TaskSupervisor, task_ref)

        # Remove from running tasks
        running_tasks = Map.delete(state.running_tasks, task_ref)
        failed_tasks = [%{task: task, reason: :cancelled, at: DateTime.utc_now()} | state.failed_tasks]

        Logger.info("Task #{task_id} cancelled")
        {:noreply, %{state | running_tasks: running_tasks, failed_tasks: failed_tasks}}

      nil ->
        # Check pending queue
        {pending, found} = remove_from_queue(state.pending_tasks, task_id)
        if found do
          Logger.info("Task #{task_id} removed from pending queue")
          {:noreply, %{state | pending_tasks: pending}}
        else
          Logger.warn("Task #{task_id} not found for cancellation")
          {:noreply, state}
        end
    end
  end

  def handle_cast(:pause_tasks, state) do
    # Stop processing new tasks from queue
    Process.send(self(), :stop_processing, [])
    Logger.info("Task processing paused")
    {:noreply, state}
  end

  def handle_cast(:resume_tasks, state) do
    # Resume processing pending tasks
    Process.send(self(), :process_pending_tasks, [])
    Logger.info("Task processing resumed")
    {:noreply, state}
  end

  # Task completion handlers
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.running_tasks, ref) do
      {{task, start_time}, running_tasks} ->
        duration = DateTime.diff(DateTime.utc_now(), start_time, :millisecond)

        # Store completion
        completed_task = %{
          task: task,
          result: result,
          duration_ms: duration,
          completed_at: DateTime.utc_now()
        }

        completed_tasks = [completed_task | state.completed_tasks]

        Logger.info("Task #{task.task_id} completed in #{duration}ms")

        # Process next pending task
        state = %{state | running_tasks: running_tasks, completed_tasks: completed_tasks}
        state = process_next_pending_task(state)

        {:noreply, state}

      nil ->
        Logger.warn("Received completion for unknown task ref: #{inspect(ref)}")
        {:noreply, state}
    end
  end

  # Task failure handler
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.running_tasks, ref) do
      {{task, start_time}, running_tasks} ->
        duration = DateTime.diff(DateTime.utc_now(), start_time, :millisecond)

        # Store failure
        failed_task = %{
          task: task,
          reason: reason,
          duration_ms: duration,
          failed_at: DateTime.utc_now()
        }

        failed_tasks = [failed_task | state.failed_tasks]

        Logger.error("Task #{task.task_id} failed after #{duration}ms: #{inspect(reason)}")

        # Process next pending task
        state = %{state | running_tasks: running_tasks, failed_tasks: failed_tasks}
        state = process_next_pending_task(state)

        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  # Process pending tasks when slots available
  def handle_info(:process_pending_tasks, state) do
    {:noreply, process_pending_tasks(state)}
  end

  # Private functions
  defp can_accept_more_tasks?(state) do
    map_size(state.running_tasks) < state.max_concurrent_tasks
  end

  defp execute_task_logic(task) do
    Logger.debug("Starting task #{task.task_id}")

    # Simulate task execution with random duration
    duration = :rand.uniform(5000) + 1000  # 1-6 seconds
    :timer.sleep(duration)


    # Return simulated result based on task type
    case task.type do
      :map ->
        # Simulate map output: list of {key, value} pairs
        Enum.map(1..:rand.uniform(10), fn i ->
          {"key_#{:rand.uniform(5)}", i}
        end)

      :reduce ->
        # Simulate reduce output: consolidated key-value pairs
        [{task.key, :rand.uniform(100)}]

      _ ->
        %{result: "completed", task_id: task.task_id}
    end
  end

  defp process_next_pending_task(state) do
    if can_accept_more_tasks?(state) and not :queue.is_empty(state.pending_tasks) do
      {{:value, task}, pending_tasks} = :queue.out(state.pending_tasks)
      task_ref = Task.Supervisor.async_nolink(MiniHadoop.Worker.TaskSupervisor, fn -> execute_task_logic(task) end)
      running_tasks = Map.put(state.running_tasks, task_ref, {task, DateTime.utc_now()})

      Logger.info("Started queued task #{task.task_id}")
      %{state | running_tasks: running_tasks, pending_tasks: pending_tasks}
      |> process_next_pending_task()  # Recursively process more if possible
    else
      state
    end
  end

  defp process_pending_tasks(state) do
    process_next_pending_task(state)
  end

  defp remove_from_queue(queue, task_id) do
    {remaining, found} = :queue.to_list(queue)
    |> Enum.split_with(fn task -> task.task_id != task_id end)

    if Enum.any?(found) do
      new_queue = :queue.from_list(remaining)
      {new_queue, true}
    else
      {queue, false}
    end
  end
end
