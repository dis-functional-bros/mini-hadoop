defmodule MiniHadoop.Job.JobRunner do
  use GenServer
  require Logger

  alias MiniHadoop.Models.ComputeTask
  alias MiniHadoop.Master.ComputeOperation

  @max_number_of_reducers Application.get_env(:mini_hadoop, :max_number_of_reducers, 10)
  @temp_path Application.get_env(:mini_hadoop, :temp_path, "/tmp/mini_hadoop")

  defstruct [
    :job,
    :map_tasks,
    :reduce_tasks,
    :shuffle_data,
    :status,
    :started_at,
    :total_map_tasks,
    :total_reduce_tasks,
    :unique_keys_by_storage,
    :worker_process_map,
    :load_balance_runner_tree
  ]

  @ets_table :job_runner_counters

  def start_link(job, participating_workers) do
    GenServer.start_link(__MODULE__, {job, participating_workers})
  end

  @impl true
  def init({job, participating_workers}) do
    :ets.new(@ets_table, [:set, :private, :named_table])
    initialize_counters()

    case start_process_on_workers(participating_workers, job.id) do
      {:ok, worker_runner_map, load_tree} ->
        state = %__MODULE__{
          job: job,
          status: :starting,
          worker_process_map: worker_runner_map,
          load_balance_runner_tree: load_tree,
          started_at: DateTime.utc_now(),
          total_map_tasks: 0,
          total_reduce_tasks: 0,
          map_tasks: [],
          reduce_tasks: [],
          shuffle_data: []
        }

        GenServer.cast(ComputeOperation, {:job_started, job.id})
        Process.send(self(), :start_processing, [])
        {:ok, state}

      {:error, reason} ->
        Logger.error("Failed to start job #{job.id}: #{inspect(reason)}")
        cleanup_after_failed_init(job.id, reason)
        {:stop, reason}
    end
  end

  defp start_process_on_workers([], _job_id), do: {:error, :no_workers_provided}

  defp start_process_on_workers(workers, job_id) do
    job_runner_pid = self()
    tasks = Enum.map(workers, fn worker_pid ->
      Task.async(fn -> start_single_worker_async(worker_pid, job_id, job_runner_pid) end)
    end)

    results = Task.await_many(tasks, 10_000)
    process_async_results(results, length(workers))
  end

  defp start_single_worker_async(worker_pid, job_id, job_runner_pid) do
    try do
      case GenServer.call(worker_pid, {:start_runner_and_storage, job_id, job_runner_pid}, 5000) do
        {:ok, result} ->
          Logger.info("Started TaskRunner and Storage on worker #{inspect(result)}")
          {:ok, result}

        {:error, reason} ->
          Logger.warn("Failed to start TaskRunner and Storage on worker #{inspect(worker_pid)}: #{inspect(reason)}")
          {:error, reason}

        other ->
          Logger.error("Unexpected response from worker #{inspect(worker_pid)}: #{inspect(other)}")
          {:error, :unexpected_response}
      end
    rescue
      error ->
        Logger.error("Error calling worker #{inspect(worker_pid)}: #{inspect(error)}")
        {:error, :communication_failed}
    end
  end

  defp process_async_results(results, total_workers) do
    {worker_runner_map, load_tree, failures} =
      Enum.reduce(results, {%{}, :gb_trees.empty(), []}, fn
        {:ok, {worker_pid, storage_pid, task_runner_pid}}, {worker_acc, tree_acc, failure_acc} ->
          # Build worker -> {storage, runner} mapping
          new_worker_acc = Map.put(worker_acc, worker_pid, {storage_pid, task_runner_pid})

          # Add to balanced tree with initial task_count = 0
          new_tree_acc = :gb_trees.insert({0, task_runner_pid}, task_runner_pid, tree_acc)

          {new_worker_acc, new_tree_acc, failure_acc}

        {:exit, reason}, {worker_acc, tree_acc, failure_acc} ->
          {worker_acc, tree_acc, [reason | failure_acc]}
      end)

    successes_count = map_size(worker_runner_map)
    failures_count = length(failures)

    cond do
      successes_count == 0 ->
        Logger.error("No TaskRunners and Storage started")
        {:error, :no_workers_started}

      failures_count > 0 ->
        Logger.warn("#{failures_count}/#{total_workers} workers failed")
        {:ok, worker_runner_map, load_tree}

      true ->
        Logger.info("Successfully started #{successes_count} TaskRunners and Storage")
        {:ok, worker_runner_map, load_tree}
    end
  end

  defp stop_all_task_runners(worker_process_map) do
    Enum.each(worker_process_map, fn {_worker_pid, {storage_pid, task_runner_pid}} ->
      stop_process(task_runner_pid)
      stop_process(storage_pid)
    end)
  end

  defp stop_process(pid) do
    # Check if PID is valid and on a connected node
    if pid_valid?(pid) do
      try do
        # Try graceful stop
        GenServer.stop(pid, :normal, 5000)
        true
      rescue
        _error ->
          # If graceful stop fails, force kill
          Process.exit(pid, :kill)
          false
      end
    else
      false
    end
  end

  defp pid_valid?(pid) do
    try do
      # This will fail if PID is from a disconnected node or invalid
      node = node(pid)
      # Check if we're connected to the node or it's local
      node == Node.self() or node in Node.list()
    rescue
      ArgumentError -> false
    end
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Job #{state.job.id} terminated: #{inspect(reason)}")

    # Stop all runners
    stop_all_task_runners(state.worker_process_map)

    # Cleanup ETS
    cleanup_ets_table()
  end

  defp cleanup_ets_table() do
    try do
      :ets.delete(@ets_table)
    rescue
      _ -> :ok
    end
  end

  defp cleanup_after_failed_init(job_id, reason) do
    Logger.info("Cleaning up failed init for job #{job_id}")

    cleanup_ets_table()

    try do
      GenServer.cast(ComputeOperation, {:job_failed, job_id, reason})
    rescue
      _ -> :ok
    end
  end

  def handle_cast({:task_completed, task_id}, state) do
    Logger.info("Task completed: #{task_id}")
    if String.starts_with?(task_id, "red_") do
      handle_reduce_task_completed(state)
    else
      handle_map_task_completed(state)
    end

    {:noreply, state}
  end

  def handle_cast({:task_failed, task_id, error}, state) do
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

  def handle_info(:start_processing, state) do
    Logger.info("Starting job processing")
    new_state = state
    |> notify_job_start()
    |> execute_map_phase()
    {:noreply, new_state}
  end

  def handle_info(:proceed_to_shuffle, state) do
    Logger.info("Proceeding to shuffle phase")
    unique_keys_by_storage = fetch_all_unique_keys(state)
    Logger.info("Unique keys structure: #{inspect(unique_keys_by_storage)}")

    new_state = %{state | unique_keys_by_storage: unique_keys_by_storage}
    {:noreply, execute_shuffle_phase(new_state)}
  end

  def handle_info(:proceed_to_completion, state) do
    Logger.info("Job #{state.job.id}: Proceeding to completion")
    reduce_results = fetch_all_reduce_results(state)
    Logger.info("Fetched #{length(reduce_results)} reduce result entries")
    Logger.info("reduce_results: #{inspect(reduce_results)}")

    # Sort the reduce results by value
    sorted_reduce_results = Enum.sort_by(reduce_results, &elem(&1, 1))

    # save to text output_files
    save_result_to_text(sorted_reduce_results)

    final_state = notify_job_completion(state)
    {:stop, :normal, final_state}
  end

  def handle_info({:job_failed, error}, state) do
    Logger.error("Job #{state.job.id} failed: #{inspect(error)}")
    final_state = notify_job_failure(state, error)
    :ets.delete(@ets_table)
    {:stop, :normal, final_state}
  end

  # Add these to catch ALL messages
  def handle_info(msg, state) do
    Logger.info("JobRunner #{inspect(self())} received ANY message: #{inspect(msg)}")
    {:noreply, state}
  end

  def handle_cast(msg, state) do
    Logger.info("JobRunner #{inspect(self())} received ANY cast: #{inspect(msg)}")

    case msg do
      {:task_completed, task_id} ->
        Logger.info("Processing task completed: #{task_id}")
        if String.starts_with?(task_id, "red_") do
          handle_reduce_task_completed(state)
        else
          handle_map_task_completed(state)
        end
      {:debug_test, task_id, from_node} ->
        Logger.info("DEBUG TEST RECEIVED from #{from_node}: #{task_id}")
      other ->
        Logger.warning("Unknown cast: #{inspect(other)}")
    end

    {:noreply, state}
  end

  defp handle_map_task_completed(state) do
    completed = :ets.update_counter(@ets_table, :completed_map_tasks, 1)

    update_progress(
      :map,
      completed,
      get_failed_map_tasks(),
      state.total_map_tasks,
      state.job.id
    )

    check_map_completion(state, completed, get_failed_map_tasks())
  end

  defp handle_reduce_task_completed(state) do
    completed = :ets.update_counter(@ets_table, :completed_reduce_tasks, 1)

    update_progress(
      :reduce,
      completed,
      get_failed_reduce_tasks(),
      state.total_reduce_tasks,
      state.job.id
    )

    check_reduce_completion(state, completed, get_failed_reduce_tasks())
  end

  defp handle_map_task_failed(state) do
    failed = :ets.update_counter(@ets_table, :failed_map_tasks, 1)

    update_progress(
      :map,
      get_completed_map_tasks(),
      failed,
      state.total_map_tasks,
      state.job.id
    )

    check_map_completion(state, get_completed_map_tasks(), failed)
  end

  defp handle_reduce_task_failed(state) do
    failed = :ets.update_counter(@ets_table, :failed_reduce_tasks, 1)

    update_progress(
      :reduce,
      get_completed_reduce_tasks(),
      failed,
      state.total_reduce_tasks,
      state.job.id
    )

    check_reduce_completion(state, get_completed_reduce_tasks(), failed)
  end

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

  defp update_progress(phase, completed, failed, total, job_id) do
    GenServer.cast(ComputeOperation, {
      :job_progress,
      job_id,
      phase,
      completed + failed,
      total
    })
  end

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

  defp execute_map_phase(state) do
    :ets.insert(@ets_table, [
      {:completed_map_tasks, 0},
      {:failed_map_tasks, 0}
    ])

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
    :ets.insert(@ets_table, [
      {:completed_reduce_tasks, 0},
      {:failed_reduce_tasks, 0}
    ])

    storage_to_unique_keys_map = state.unique_keys_by_storage
    # =================================  TODO selesaikan logikanya udah ada datanya
    Logger.info("Starting shuffle phase")
    shuffle_result = [{"key", []}, {"key", []}, {"key", []}]
    # =================================

    %{state | shuffle_data: shuffle_result, status: :shuffle_completed}
    |> execute_reduce_phase()
  end

  defp generate_map_tasks(state) do
    # ============================== TODO generate map tasks
    # Lebih simple map task dibuat untuk tiap block
    # Pastikan input data dalam bentuk format {"block_id", ["worker_pid", "worker_pid"]}
    worker_pids = ComputeOperation.get_workers()

    map_tasks = 1..10
      |> Enum.map(fn task_id ->
        worker_index = rem(task_id - 1, length(worker_pids))
        worker_pid = Enum.at(worker_pids, worker_index)
        block_info = {"block_#{task_id}", [worker_pid]}
        ComputeTask.new_map(state.job.id, block_info, state.job.map_module)
      end)

    Logger.info("Generated #{length(map_tasks)} map tasks")
    %{state | map_tasks: map_tasks}
  end

  defp generate_reduce_tasks(state) do

  # =============================== TODO generate reduce tasks
  # Constraint Generate min(@max_number_of_reducers, length(state.shuffle_data))
  # Kita membuat reduce tasks itu jumlahnya di kasih batas atas  @max_number_of_reducers
  # Yang paling utama itu argument ke 2 new reduce tasks yang merupakan
  # List of key-worker_pid pairs yang menunjukan bahwa reducer dapat list key ini dan tiap
  # Key tersebut dapat diambil di sini


    reduce_tasks = state.shuffle_data
      |> Enum.map(fn {key, storage_pids} ->
        ComputeTask.new_reduce(
          state.job.id,
          [{key, worker_pids}],
          state.job.reduce_module
        )
      end)
  # ===============================

    Logger.info("Generated #{length(reduce_tasks)} reduce tasks")
    %{state | reduce_tasks: reduce_tasks}
  end

  defp dispatch_map_tasks(state) do
    {dispatched, failed, _assignments} = dispatch_tasks(state.map_tasks, {0, 0, {state.worker_process_map, state.load_balance_runner_tree}})
    Logger.info("Dispatched #{dispatched} map tasks, #{failed} failed")

    %{state | map_tasks: nil, total_map_tasks: dispatched}
  end

  defp dispatch_reduce_tasks(state) do
    {dispatched, failed, _assignments} = dispatch_tasks(state.reduce_tasks, {0, 0, {state.worker_process_map, state.load_balance_runner_tree}})
    Logger.info("Dispatched #{dispatched} reduce tasks, #{failed} failed")

    %{state | reduce_tasks: nil, total_reduce_tasks: dispatched}
  end

  defp wait_for_map_completion(state) do
    Logger.info("Waiting for map tasks to complete #{inspect(self())}")
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

  defp notify_phase_start(state, phase) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.id, phase})
    state
  end

  defp notify_job_start(state) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.id, :starting})
    state
  end

  defp notify_job_completion(state) do
    results = generate_final_results(state)
    GenServer.cast(ComputeOperation, {:job_completed, state.job.id, results})
    %{state | status: :completed}
  end

  defp notify_job_failure(state, error) do
    GenServer.cast(ComputeOperation, {:job_failed, state.job.id, error})
    %{state | status: :failed}
  end

  defp dispatch_tasks([], acc), do: acc

  defp dispatch_tasks([task | remaining_tasks], {dispatched, failed, {worker_process_map, load_balance_runner_tree}}) do
    case execute_task(task, worker_process_map, load_balance_runner_tree) do
      {:ok, new_tree} ->
        dispatch_tasks(remaining_tasks, {dispatched + 1, failed, {worker_process_map, new_tree}})

      {:ok, :all_workers_queue_full} ->
        Logger.debug("All workers queue are full, retrying")
        dispatch_tasks([task | remaining_tasks], {dispatched, failed, {worker_process_map, load_balance_runner_tree}})

      {:error, reason} ->
        Logger.error("Failed to dispatch task #{task.task_id}: #{reason}")
        dispatch_tasks(remaining_tasks, {dispatched, failed + 1, {worker_process_map, load_balance_runner_tree}})

      {:error, :no_worker} ->
        remaining_count = length(remaining_tasks)
        dispatch_tasks([], {dispatched, failed + 1 + remaining_count, {worker_process_map, load_balance_runner_tree}})
    end
  end

  defp generate_final_results(state) do
    %{
      total_map_tasks: state.total_map_tasks,
      total_reduce_tasks: state.total_reduce_tasks,
      output_files: [
        "/placeholder/result/#{state.job.id}.txt"
      ]
    }
  end

  defp execute_task(task, worker_process_map, load_balance_runner_tree) do
    choose_worker(task, worker_process_map, load_balance_runner_tree)
    |> then(fn
      {:ok, runner_pid, new_tree} ->
        GenServer.cast(runner_pid, {:execute_task, task})
        {:ok, new_tree}

      {:error, reason} ->
        {:error, reason}
    end)
  end

  defp fetch_all_unique_keys(state) do
    storage_pids = Map.values(state.worker_process_map)
                   |> Enum.map(fn {storage_pid, _runner_pid} -> storage_pid end)

    storage_pids
    |> Task.async_stream(
      fn storage_pid ->
        case GenServer.call(storage_pid, :get_unique_keys, 10_000) do
          {:ok, keys} -> {:ok, {storage_pid, keys}}
          {:error, reason} -> {:error, {storage_pid, reason}}
        end
      end,
      max_concurrency: length(storage_pids),
      timeout: 15_000
    )
    |> Enum.reduce(%{}, fn
      {:ok, {:ok, {storage_pid, keys}}}, acc ->
        Map.put(acc, storage_pid, keys)

      {:ok, {:error, {storage_pid, reason}}}, acc ->
        Logger.warn("Failed to fetch keys from #{inspect(storage_pid)}: #{inspect(reason)}")
        acc

      {:exit, reason}, acc ->
        Logger.warn("Task exited: #{inspect(reason)}")
        acc
    end)
  end

  defp fetch_all_reduce_results(state) do
    storage_pids = Map.values(state.worker_process_map)
                   |> Enum.map(fn {storage_pid, _runner_pid} -> storage_pid end)

    storage_pids
    |> Task.async_stream(
      fn storage_pid ->
        case GenServer.call(storage_pid, :get_reduce_results, 10_000) do
          {:ok, results} when is_list(results) ->
            key_value_tuples = Enum.map(results, fn {k, v} -> {k, v} end)
            {:ok, {storage_pid, key_value_tuples}}

          {:ok, other} ->
            {:error, {storage_pid, {:invalid_format, other}}}

          {:error, reason} ->
            {:error, {storage_pid, reason}}
        end
      end,
      max_concurrency: length(storage_pids),
      timeout: 15_000
    )
    |> Enum.reduce([], fn
      {:ok, {:ok, {storage_pid, key_value_tuples}}}, acc ->
        key_value_tuples ++ acc

      {:ok, {:error, {storage_pid, reason}}}, acc ->
        Logger.warn("Failed to fetch reduce results from #{inspect(storage_pid)}: #{inspect(reason)}")
        acc

      {:exit, reason}, acc ->
        Logger.warn("Task exited: #{inspect(reason)}")
        acc
    end)
  end

  defp save_result_to_text(sorted_reduce_results, state) do
    # save file to
  end

  defp choose_worker(%{type: :map, input_data: {_block_id, [worker_pid | _]}}, worker_process_map, load_balance_runner_tree) do
    # =================================
    case Map.get(worker_process_map, worker_pid) do
      {_storage_pid, runner_pid} ->
        # Find the current task count for this runner
        case get_runner_from_tree(load_balance_runner_tree, runner_pid) do
          {{current_count, ^runner_pid}, _value} ->
            # Remove old entry and create new entry with incremented count
            old_tree = :gb_trees.delete({current_count, runner_pid}, load_balance_runner_tree)
            new_tree = :gb_trees.insert({current_count + 1, runner_pid}, runner_pid, old_tree)
            {:ok, runner_pid, new_tree}

          nil ->
            {:error, :runner_not_found_in_tree}
        end

      nil ->
        {:error, :worker_not_found}
    end
  end

  defp choose_worker(%{type: :reduce}, _worker_process_map, load_balance_runner_tree) do
    case :gb_trees.is_empty(load_balance_runner_tree) do
      true ->
        {:error, :no_runners_available}

      false ->
        # Take the least loaded runner
        {{current_count, runner_pid}, _value, remaining_tree} = :gb_trees.take_smallest(load_balance_runner_tree)

        # Insert back with incremented count
        new_tree = :gb_trees.insert({current_count + 1, runner_pid}, runner_pid, remaining_tree)
        {:ok, runner_pid, new_tree}
    end
  end

  defp choose_worker(_task, _worker_process_map, _load_balance_runner_tree) do
    {:error, "Invalid task structure"}
  end

  defp get_runner_from_tree(tree, target_runner_pid) do
    iterator = :gb_trees.iterator(tree)
    find_runner_in_iterator(iterator, target_runner_pid)
  end

  defp find_runner_in_iterator(iterator, target_runner_pid) do
    case :gb_trees.next(iterator) do
      {{count, runner_pid} = key, value, next_iterator} ->
        if runner_pid == target_runner_pid do
          {key, value}
        else
          find_runner_in_iterator(next_iterator, target_runner_pid)
        end
      :none ->
        nil
    end
  end
end
