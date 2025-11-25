defmodule MiniHadoop.ComputeTask.TaskRunner do
  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    {:ok, %{
      max_concurrent_tasks: opts[:max_concurrent_compute_tasks] || 1,
      current_tasks: 0,  # Counter running tasks
      pending_tasks: :queue.new(),
      worker_node_pid: opts[:worker_node_pid]
    }}
  end

  def handle_call(:get_status, _from, state) do
    status = %{
      running: state.current_tasks,
      pending: :queue.len(state.pending_tasks),
      max_concurrent: state.max_concurrent_tasks
    }
    {:reply, status, state}
  end

  def handle_cast({:execute_task, task}, state) do
    state
    |> maybe_execute_task(task)
    |> then(fn new_state -> {:noreply, new_state} end)
  end

  # Task completion with task info included in result
  def handle_info({ref, {task, result}}, state) when is_reference(ref) do
    state
    |> decrement_task_count()
    |> save_result(task, result)
    |> notify_completion(task)
    |> process_next_pending_task()
    |> then(fn new_state -> {:noreply, new_state} end)
  end

  # Task failure handler
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
    state
    |> decrement_task_count()
    |> process_next_pending_task()
    |> then(fn new_state -> {:noreply, new_state} end)
  end

  # Process pending tasks when slots available
  def handle_info(:process_pending_tasks, state) do
    {:noreply, process_next_pending_task(state)}
  end

  # Private functional helpers

  defp maybe_execute_task(state, task) do
    if can_accept_more_tasks?(state) do
      execute_task_immediately(state, task)
    else
      queue_task(state, task)
    end
  end

  defp execute_task_immediately(state, task) do
    task_execution = fn ->
      result = execute_task_logic(task)
      {task, result}  # Return task with result for functional handling
    end

    Task.Supervisor.async_nolink(MiniHadoop.ComputeTask.TaskSupervisor, task_execution)

    state
    |> increment_task_count()
  end

  defp queue_task(state, task) do
    pending_tasks = :queue.in(task, state.pending_tasks)
    Logger.info("Task #{task.task_id} queued. #{:queue.len(pending_tasks)} tasks pending")
    %{state | pending_tasks: pending_tasks}
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

  defp can_accept_more_tasks?(state) do
    state.current_tasks < state.max_concurrent_tasks
  end

  defp increment_task_count(state) do
    %{state | current_tasks: state.current_tasks + 1}
  end

  defp decrement_task_count(state) do
    %{state | current_tasks: state.current_tasks - 1}
  end

  defp notify_completion(state, task) do
    # Side effect at the boundary
    send(task.job_ref, {:task_completed, task.task_id})

    # TODO
    # Save result to worker temp_storage
    Logger.info("Task #{task.task_id} completed")
    state
  end

  defp save_result(state, task, result) do
    # Save result to worker temp_storage
    # TODO: Logic to save result to worker temp_storage
    Logger.info("Task #{task.task_id} completed, result: #{inspect(result)}")
    state
  end

  defp process_next_pending_task(state) do
    if can_accept_more_tasks?(state) do
      case dequeue_task(state) do
        {task, new_state} when not is_nil(task) ->
          new_state
          |> execute_task_immediately(task)
          |> process_next_pending_task()  # Recursively process more
        {nil, new_state} ->
          new_state
      end
    else
      state
    end
  end

  defp dequeue_task(state) do
    case :queue.out(state.pending_tasks) do
      {{:value, task}, pending_tasks} ->
        {task, %{state | pending_tasks: pending_tasks}}
      {:empty, _} ->
        {nil, state}
    end
  end
end
