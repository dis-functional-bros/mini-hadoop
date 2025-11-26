defmodule MiniHadoop.ComputeTask.TaskRunner do
  use GenServer
  require Logger

  @ets_table :task_runner_counters
  @task_refs_table :task_runner_refs

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    # Create ETS table for atomic counters
    :ets.new(@ets_table, [:set, :private, :named_table])
    :ets.insert(@ets_table, {:current_tasks, 0})

    # Create ETS table for task reference tracking
    :ets.new(@task_refs_table, [:set, :private, :named_table])

    {:ok, %{
      max_concurrent_tasks: opts[:max_concurrent_compute_tasks] || 1,
      pending_tasks: :queue.new(),
      worker_node_pid: opts[:worker_node_pid]
    }}
  end

  def handle_call(:get_status, _from, state) do
    [{:current_tasks, current}] = :ets.lookup(@ets_table, :current_tasks)

    status = %{
      running: current,
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

  # Update the completion handler
  def handle_info({task_ref, {ref, task, result}}, state) when is_reference(task_ref) do
    # Clean up our custom ref
    :ets.delete(@task_refs_table, ref)

    state
    |> decrement_task_count()
    |> save_result(task, result)
    |> notify_completion(task)
    |> process_next_pending_task()
    |> then(fn new_state -> {:noreply, new_state} end)
  end

  # Successful completion - ignore
  def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
    {:noreply, state}
  end
  # Task failure handler
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
    # Look up task info from ETS and notify failure
    case :ets.lookup(@task_refs_table, ref) do
      [{^ref, job_ref, task_id}] ->
        # Clean up the reference tracking
        :ets.delete(@task_refs_table, ref)

        state
        |> decrement_task_count()
        |> notify_failure(job_ref, task_id, reason)
        |> process_next_pending_task()

      [] ->
        # Unknown reference, just decrement counter
        Logger.error("Task failure for unknown reference: #{inspect(reason)}")
        state
        |> decrement_task_count()
        |> process_next_pending_task()
    end
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
    # Create a unique reference first
    ref = make_ref()

    # Store in ETS BEFORE any task starts
    :ets.insert(@task_refs_table, {ref, task.job_ref, task.task_id})

    # Now start the task
    task_async = Task.Supervisor.async_nolink(MiniHadoop.ComputeTask.TaskSupervisor, fn ->
      result = execute_task_logic(task)
      {ref, task, result}  # Include our custom ref in the result
    end)

    state
    |> increment_task_count()
  end


  defp queue_task(state, task) do
    pending_tasks = :queue.in(task, state.pending_tasks)
    Logger.info("Task #{task.task_id} queued. #{:queue.len(pending_tasks)} tasks pending")
    %{state | pending_tasks: pending_tasks}
  end

  defp execute_task_logic(task) do
    duration = :rand.uniform(5000) + 1000  # 1-6 seconds
    :timer.sleep(duration)

    # Return simulated result based on task type
    case task.type do
      :map ->

        # TODO
        # prepare map_input from input_data in task.input_data

        # Dummy for testing if coordination is working
        map_input = "Dummy data dummy data dummy data dummy data dummy data dummy data"
        execute_map_task(map_input, task.function, [])


      :reduce ->
        # TODO
        # Prepare reduce_data from intermediate_data in task.input_data

        # Dummy for testing if coordination is working
        reduce_input = %{"dummy"=>[4,5,7,3], "data"=>[1,2,3,4], "more_data"=>[5,6,7,8]}
        execute_reduce_task(reduce_input, task.function, [])
      _ ->
        %{result: "completed", task_id: task.task_id}
    end
  end

  @spec execute_map_task(any(), (any() -> [{any(), any()}]), any()) :: %{any() => [any()]}
  defp execute_map_task(input, map_function, additional_context) do
    #TODO
    # Implement the map task make it so that is extensible

    # dummy result
    %{"dummy": [2, 4], "data": [1, 3]}
  end

  @spec execute_reduce_task(any(), (any() -> [{any(), any()}]), any()) :: %{any() => any()}
  defp execute_reduce_task(input, reduce_function, additional_context) do
    #TODO
    # Implement the reduce task make it so that is extensible

    # dummy result
    %{"dummy": 6, "data": 4}
  end

  defp can_accept_more_tasks?(state) do
    [{:current_tasks, current}] = :ets.lookup(@ets_table, :current_tasks)
    current < state.max_concurrent_tasks
  end

  defp increment_task_count(state) do
    :ets.update_counter(@ets_table, :current_tasks, 1)
    state
  end

  defp decrement_task_count(state) do
    :ets.update_counter(@ets_table, :current_tasks, -1)
    state
  end

  defp notify_completion(state, task) do
    # Side effect at the boundary
    send(task.job_ref, {:task_completed, task.task_id})

    # TODO
    # Save result to worker temp_storage
    state
  end

  defp notify_failure(state, job_ref, task_id, reason) do
    # Side effect at the boundary
    send(job_ref, {:task_failed, task_id, reason})

    Logger.error("Task #{task_id} failed with reason: #{inspect(reason)}")
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
