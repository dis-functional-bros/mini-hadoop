defmodule MiniHadoop.ComputeTask.TaskRunner do
  use GenServer
  require Logger

  @max_concurrent_tasks_on_runner Application.get_env(:mini_hadoop, :max_concurrent_compute_tasks, 4)
  @max_queue_size_of_task_runner Application.get_env(:mini_hadoop, :max_queue_size_of_task_runner, 20)

  def start_link(job_id, job_pid, storage_pid) do
    GenServer.start_link(__MODULE__, {job_id, job_pid, storage_pid})
  end

  @impl true
  def init({job_id, job_pid, storage_pid}) do
    # Create anonymous ETS tables
    counters_table = :ets.new(nil, [:set, :private])
    :ets.insert(counters_table, {:current_tasks, 0})

    task_refs_table = :ets.new(nil, [:set, :private])

    {:ok, %{
      job_id: job_id,
      job_pid: job_pid,
      storage_pid: storage_pid,
      pending_tasks: :queue.new(),
      counters_table: counters_table,      # Store table references
      task_refs_table: task_refs_table
    }}
  end

  def handle_call(:get_status, _from, state) do
    [{:current_tasks, current}] = :ets.lookup(state.counters_table, :current_tasks)

    status = %{
      running: current,
      pending: :queue.len(state.pending_tasks),
    }
    {:reply, status, state}
  end

  # When: Task arrive
  def handle_cast({:execute_task, task}, state) do
    if concurrent_tasks_under_limit?(state) do
      {:noreply, execute_task_immediately(state, task)}
    else
      {:noreply, queue_task(state, task)}
    end
  end

   # When: Task completes successfully
   def handle_info({task_ref, {ref, task, result}}, state) when is_reference(task_ref) do
     :ets.delete(state.task_refs_table, ref)
     {:noreply, handle_task_completion(state, task, result)}
   end

   # When: Normal process shutdown (ignore)
   def handle_info({:DOWN, _ref, :process, _pid, :normal}, state) do
     {:noreply, state}
   end

   # When: Task fails with reason
   def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
     case :ets.lookup(state.task_refs_table, ref) do
       [{^ref, task_id}] ->
         :ets.delete(state.task_refs_table, ref)
         {:noreply, handle_task_failure(state, task_id, reason)}
       [] ->
         Logger.error("Task failure for unknown reference: #{inspect(reason)}")
         {:noreply, decrement_task_count(state) |> process_next_pending_task()}
     end
   end

  # Private functional helpers
  defp handle_task_completion(state, task, result) do
    state
    |> decrement_task_count()
    |> save_result(task, result)
    |> notify_completion(task)
    |> process_next_pending_task()
  end

  defp handle_task_failure(state, task_id, reason) do
    state
    |> decrement_task_count()
    |> notify_failure(task_id, reason)
    |> process_next_pending_task()
  end

  defp execute_task_immediately(state, task) do
    # Create a unique reference first
    ref = make_ref()

    # Store in ETS BEFORE any task starts
    :ets.insert(state.task_refs_table, {ref, task.id})

    # Now start the task
    task_async = Task.Supervisor.async_nolink(MiniHadoop.ComputeTask.TaskSupervisor, fn ->
      result = execute_task_logic(task)
      {ref, task, result}
    end)

    # Monitor the task
    ref = Process.monitor(task_async.pid)
    :ets.insert(state.task_refs_table, {ref, task.id})

    increment_task_count(state)
  end

  defp queue_task(state, task) do
    pending_tasks = :queue.in(task, state.pending_tasks)
    Logger.info("Task #{task.id} queued. #{:queue.len(pending_tasks)} tasks pending")
    %{state | pending_tasks: pending_tasks}
  end

  defp execute_task_logic(task) do
    Process.sleep(4000)
    case fetch_task_data(task) do
      {:ok, :map, map_input} ->
        execute_map_task(map_input, task.module, %{})
      {:ok, :reduce, reduce_input} ->
        execute_reduce_task(reduce_input, task.module, %{})
      {:error, reason} ->
        %{result: "failed", task_id: task.id, error: reason}
    end
  end

  defp fetch_task_data(task) do
    case task.type do
      :map -> {:ok, :map, "Dummy data dummy data dummy data dummy data dummy data dummy data"}
      :reduce -> {:ok, :reduce, %{"dummy"=>[4,5,7,3], "data"=>[1,2,3,4], "more_data"=>[5,6,7,8]}}
    end
  end

  @spec execute_map_task(any(), module(), any()) :: list()
  def execute_map_task(input, map_module, additional_context) do
    case MiniHadoop.Map.MapBehaviour.execute(map_module, input, additional_context) do
      {:ok, result} ->
        result
      {:error, reason} ->
        raise "Map task failed: #{inspect(reason)}"
    end
  end

  @spec execute_reduce_task(any(), module(), any()) :: list()
  def execute_reduce_task(input, reduce_module, additional_context) do
    case MiniHadoop.Reduce.ReduceBehaviour.execute(reduce_module, input, additional_context) do
      {:ok, result} ->
        result
      {:error, reason} ->
        raise "Reduce task failed: #{inspect(reason)}"
    end
  end

  defp concurrent_tasks_under_limit?(state) do
    [{:current_tasks, current}] = :ets.lookup(state.counters_table, :current_tasks)
    current < @max_concurrent_tasks_on_runner
  end

  defp increment_task_count(state) do
    :ets.update_counter(state.counters_table, :current_tasks, 1)
    state
  end

  defp decrement_task_count(state) do
    :ets.update_counter(state.counters_table, :current_tasks, -1)
    state
  end

  defp notify_completion(state, task) do
    GenServer.cast(state.job_pid, {:task_completed, task.id})
    state
  end

  defp notify_failure(state, task_id, reason) do
    GenServer.cast(state.job_pid, {:task_failed, task_id, reason})
    Logger.error("Task #{task_id} failed with reason: #{inspect(reason)}")
    state
  end

  defp save_result(state, task, result) do
    try do
      case task.type do
        :map ->
          :ok = GenServer.call(state.storage_pid, {:store_map_results, result})
          Logger.info("Map task #{task.id} results saved to storage")

        :reduce ->
          :ok = GenServer.call(state.storage_pid, {:store_reduce_results, result})
          Logger.info("Reduce task #{task.id} saved #{length(result)} results to storage")
      end
    rescue
      error ->
        Logger.error("Failed to save task #{task.id} results: #{inspect(error)}")
        send(state.job_pid, {:task_storage_failed, task.id, error})
    end

    state
  end

  defp process_next_pending_task(state) do
    if concurrent_tasks_under_limit?(state) do
      case dequeue_task(state) do
        {task, new_state} when not is_nil(task) ->
          new_state
          |> execute_task_immediately(task)
          |> process_next_pending_task()  # Recursively process task
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

  @impl true
  def terminate(reason, state) do
    Logger.info("TaskRunner stopping for job #{state.job_id}, reason: #{inspect(reason)}")
    {:stop, :normal, :ok, state}
  end
end
