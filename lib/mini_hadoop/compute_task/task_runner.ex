defmodule MiniHadoop.ComputeTask.TaskRunner do
  use GenServer
  require Logger

  alias MiniHadoop.Models.ComputeTask

  @max_concurrent_tasks_on_runner Application.compile_env(:mini_hadoop, :max_concurrent_tasks_on_runner, 4)

  def start_link(job_id, job_pid, storage_pid) do
    GenServer.start_link(__MODULE__, {job_id, job_pid, storage_pid})
  end

  @impl true
  def init({job_id, job_pid, storage_pid}) do
    counters_table = :ets.new(nil, [:set, :private])
    :ets.insert(counters_table, {:current_tasks, 0})

    task_refs_table = :ets.new(nil, [:set, :private])

    {:ok, %{
      job_id: job_id,
      job_pid: job_pid,
      storage_pid: storage_pid,
      pending_tasks: :queue.new(),
      counters_table: counters_table,
      task_refs_table: task_refs_table
    }}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    [{:current_tasks, current}] = :ets.lookup(state.counters_table, :current_tasks)

    status = %{
      running: current,
      pending: :queue.len(state.pending_tasks),
    }
    {:reply, status, state}
  end

  # When: Task arrive
  @impl true
  def handle_info({:execute_task, task}, state) do
    if concurrent_tasks_under_limit?(state) do
      {:noreply, execute_task_immediately(state, task)}
    else
      {:noreply, queue_task(state, task)}
    end
  end

   # When: Task completes successfully
   def handle_info({task_ref, {:success, ref, task}}, state) when is_reference(task_ref) do
     :ets.delete(state.task_refs_table, ref)
     {:noreply, handle_task_completion(state, task)}
   end

   # When: Expected task failure
   def handle_info({task_ref, {:error, ref, task}}, state) when is_reference(task_ref) do
     :ets.delete(state.task_refs_table, ref)
     {:noreply, handle_task_failure(state, task.id, task.error)}
   end

   # When: Normal process shutdown for completed tasks (cleanup)
   def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
     case :ets.lookup(state.task_refs_table, ref) do
       [{^ref, _task_id}] ->
         # Still in ETS? Clean it up (shouldn't happen but defensive)
         :ets.delete(state.task_refs_table, ref)
         {:noreply, state}
       [] ->
         # Already cleaned up - perfect
         {:noreply, state}
     end
   end

   # When: Unexpected process shutdown
   def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
     case :ets.lookup(state.task_refs_table, ref) do
       [{^ref, task_id}] ->
         :ets.delete(state.task_refs_table, ref)
         {:noreply, handle_task_failure(state, task_id, reason )}
       [] ->
         Logger.error("Task failure for unknown reference: #{inspect(reason)}")
         {:noreply, decrement_task_count(state) |> process_next_pending_task()}
     end
   end

  # Private functional helpers
  defp handle_task_completion(state, task) do
    state
    |> decrement_task_count()
    |> save_result(task)
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
      case execute_task_logic(ComputeTask.mark_started(task)) do
        {:ok, task} -> {:success, ref, task}
        {:error,task} -> {:error, ref, task}
      end
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
    case MiniHadoop.ComputeTask.TaskExecutor.execute_task(task) do
      {:ok, completed_task} ->
        {:ok, completed_task}

      {:error, {:user_function_error, reason}} ->
        failed_task = ComputeTask.mark_failed(task, "User function error: #{reason}")
        {:error, failed_task}

      {:error, {:user_function_crashed, error_msg}} ->
        failed_task = ComputeTask.mark_failed(task, "Function crashed: #{error_msg}")
        {:error, failed_task}

      {:error, {:data_fetch_failed, reason}} ->
        failed_task = ComputeTask.mark_failed(task, "Data fetch failed: #{reason}")
        {:error, failed_task}

      {:error, {:invalid_result_format, details}} ->
        failed_task = ComputeTask.mark_failed(task, "Invalid result: #{details}")
        {:error, failed_task}

      {:error, {:unexpected_return_type, actual}} ->
        failed_task = ComputeTask.mark_failed(task, "Unexpected return: #{inspect(actual)}")
        {:error, failed_task}

      {:error, reason} ->
        failed_task = ComputeTask.mark_failed(task, "Execution failed: #{inspect(reason)}")
        {:error, failed_task}
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

  defp save_result(state, task) do
    try do
      case task.type do
        :map ->
          :ok = GenServer.call(state.storage_pid, {:store_map_results, task.output_data}, :infinity)
          Logger.info("Map task #{task.id} results saved to storage")

        :reduce ->
          :ok = GenServer.call(state.storage_pid, {:store_reduce_results, task.output_data}, :infinity)
          Logger.info("Reduce task #{task.id} saved #{length(task.output_data)} results to storage")
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
