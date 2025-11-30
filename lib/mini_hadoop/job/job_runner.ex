defmodule MiniHadoop.Job.JobRunner do
  use GenServer
  require Logger

  alias MiniHadoop.Models.ComputeTask
  alias MiniHadoop.Master.ComputeOperation

  @max_key_each_reduce_task Application.compile_env(:mini_hadoop, :max_num_of_key_each_reduce_task)

  defstruct [
    :job,
    :map_tasks,
    :reduce_tasks,
    :participating_blocks,
    :shuffle_data,
    :status,
    :started_at,
    :total_map_tasks,
    :total_reduce_tasks,
    :worker_process_map,
    :load_balance_runner_tree,
    :job_spawned_task_counter_ref,
    :result_path
  ]

  def start_link(job, participating_workers) do
    GenServer.start_link(__MODULE__, {job, participating_workers})
  end

  @impl true
  def init({job, participating_workers}) do

    result_path = Application.get_env(:mini_hadoop, :data_base_path, "/app/data/")

    job_spawned_task_counter_ref = :ets.new(nil, [:set, :private])
    initialize_counters(job_spawned_task_counter_ref)

    case start_process_on_workers(participating_workers, job.id) do
      {:ok, worker_runner_map, load_tree} ->
        state = %__MODULE__{
          job: job,
          map_tasks: [],
          reduce_tasks: [],
          participating_blocks: [],
          shuffle_data: [],
          worker_process_map: worker_runner_map,
          load_balance_runner_tree: load_tree,
          started_at: DateTime.utc_now(),
          total_map_tasks: 0,
          total_reduce_tasks: 0,
          map_tasks: [],
          reduce_tasks: [],
          job_spawned_task_counter_ref: job_spawned_task_counter_ref,
          result_path: job.output_dir || result_path
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

  # Higher-order phase execution
  defp execute_phase(state, phase_type, data_fetcher, task_generator, task_dispatcher, completion_checker) do
    with_counter_cleanup(state, phase_type, fn state ->
      state
      |> notify_phase_start(phase_type)
      |> data_fetcher.()
      |> (fn state_with_data ->
            {data_list, chunk_size} = case phase_type do
              :map ->
                {Map.get(state_with_data, :participating_blocks), Map.size(state_with_data.worker_process_map)* 5}

              :reduce ->
                reduce_chunk_size = @max_key_each_reduce_task * Map.size(state_with_data.worker_process_map) * 5
                {Map.get(state_with_data, :shuffle_data), reduce_chunk_size}
            end

            # Lazyly evaluated
            data_list
            |> Stream.chunk_every(chunk_size)
            |> Stream.each(fn chunk ->
              Task.start(fn ->
                state_with_data
                |> Map.put(:current_chunk, chunk)
                |> then(&task_generator.(&1))
                |> then(&task_dispatcher.(&1))
              end)
            end)
            |> Stream.run()
            state_with_data
          end).()
      |> completion_checker.()
    end)
  end

  defp with_counter_cleanup(state, phase_type, fun) do
    completed_key = :"completed_#{phase_type}_tasks"
    :ets.insert(state.job_spawned_task_counter_ref, [{completed_key, 0}, {:failed_tasks, 0}])

    try do
      fun.(state)
    rescue
      error ->
        Process.send(self(), {:job_failed, error}, [])
        state
    end
  end

  # Generic task completion handler
  defp handle_task_completion(state, task_type, completed_key, failed_key, total_tasks_field, next_phase) do
    completed = :ets.update_counter(state.job_spawned_task_counter_ref, completed_key, 1)
    failed = get_counter(state.job_spawned_task_counter_ref, failed_key)
    total = Map.get(state, total_tasks_field)

    update_progress(task_type, completed, failed, total, state.job.id)
    check_completion(state, task_type, completed, failed, total, next_phase)
  end

  defp check_completion(state, phase_type, completed, failed, total, next_phase) do
    if completed + failed >= total do
      Logger.info("All #{phase_type} tasks completed (completed: #{completed}, failed: #{failed})")
      Process.send(self(), next_phase, [])
    end
    state
  end

  # Message handlers using higher-order functions
  def handle_cast({:task_completed, task_id}, state) do
    Logger.info("Task completed: #{task_id}")

    task_handler = if String.starts_with?(task_id, "red_") do
      &handle_reduce_task_completed/1
    else
      &handle_map_task_completed/1
    end

    {:noreply, task_handler.(state)}
  end

  def handle_cast({:task_failed, task_id, error}, state) do
    Logger.error("Task failed: #{task_id}, error: #{error}")

    task_handler = if String.starts_with?(task_id, "red_") do
      &handle_reduce_task_failed/1
    else
      &handle_map_task_failed/1
    end

    {:noreply, task_handler.(state)}
  end

  def handle_info(:start_processing, state) do
    Logger.info("Starting job processing")
    new_state = state
    |> notify_job_start()
    |> execute_map_phase()
    {:noreply, new_state}
  end

  def handle_info(:proceed_to_reduce, state) do
    Logger.info("Proceeding to reduce phase")
    {:noreply, execute_reduce_phase(state)}
  end

  def handle_info(:proceed_to_completion, state) do
    Logger.info("Job #{state.job.id}: Proceeding to completion")

    result = state
      |> fetch_all_reduce_results()
      |> process_reduce_results()
      |> write_results_to_file(state.result_path, state.job.id)
      |> create_job_summary(state)
    final_state = notify_job_completion(state, result)
    {:stop, :normal, final_state}
  end

  defp process_reduce_results(reduce_results) do
    # reduce result
    Enum.sort_by(reduce_results, fn {_key, value} -> value end, :desc)
  end

  defp write_results_to_file(reduce_results, result_path, job_id) do
    file_path = Path.join(result_path, "result_#{job_id}.txt")
    directory = Path.dirname(file_path)
    File.mkdir_p!(directory)

    reduce_results
    |> Enum.map(fn {key, value} -> %{key => value} end)  # Convert to maps
    |> Jason.encode!(pretty: true)
    |> then(&File.write(file_path, &1))
    |> case do
      :ok ->
        Logger.info("Job #{job_id}: Results written to #{file_path}")
        file_path

      {:error, reason} ->
        Logger.error("Job #{job_id}: Failed to write results to file: #{reason}")
        nil
    end
  end

  defp create_job_summary(results_file_path, state) do
    %{
      total_map_tasks: state.total_map_tasks,
      total_reduce_tasks: state.total_reduce_tasks,
      job_results: results_file_path
    }
  end


  def handle_info({:job_failed, error}, state) do
    Logger.error("Job #{state.job.id} failed: #{inspect(error)}")
    final_state = notify_job_failure(state, error)
    {:stop, :normal, final_state}
  end

  def handle_info({:DOWN, _ref, :process, dead_worker_pid, reason}, state) do
    Logger.error("Worker died: #{inspect(dead_worker_pid)}, reason: #{reason}")
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.info("JobRunner #{inspect(self())} received ANY message: #{inspect(msg)}")
    {:noreply, state}
  end

  # Phase execution with higher-order functions
  defp execute_map_phase(state) do
    execute_phase(
      state,
      :map,
      &fetch_participating_blocks/1,
      &generate_map_tasks/1,
      &dispatch_tasks_with_state(&1, :map_tasks),
      &wait_for_phase_completion(&1, :map, :proceed_to_reduce)
    )
  end

  defp execute_reduce_phase(state) do
    execute_phase(
      state,
      :reduce,
      &fetch_shuffle_data/1,
      &generate_reduce_tasks/1,
      &dispatch_tasks_with_state(&1, :reduce_tasks),
      &wait_for_phase_completion(&1, :reduce, :proceed_to_completion)
    )
  end

  # Task completion handlers
  defp handle_map_task_completed(state) do
    handle_task_completion(state, :map, :completed_map_tasks, :failed_map_tasks, :total_map_tasks, :proceed_to_reduce)
  end

  defp handle_reduce_task_completed(state) do
    handle_task_completion(state, :reduce, :completed_reduce_tasks, :failed_reduce_tasks, :total_reduce_tasks, :proceed_to_completion)
  end

  defp handle_map_task_failed(state) do
    failed = :ets.update_counter(state.job_spawned_task_counter_ref, :failed_map_tasks, 1)
    completed = get_counter(state.job_spawned_task_counter_ref, :completed_map_tasks)
    check_completion(state, :map, completed, failed, state.total_map_tasks, :proceed_to_reduce)
  end

  defp handle_reduce_task_failed(state) do
    failed = :ets.update_counter(state.job_spawned_task_counter_ref, :failed_reduce_tasks, 1)
    completed = get_counter(state.job_spawned_task_counter_ref, :completed_reduce_tasks)
    check_completion(state, :reduce, completed, failed, state.total_reduce_tasks, :proceed_to_completion)
  end

  # TODO
  # ======================================================
  @spec fetch_participating_blocks(%{job: %{input_data: [String.t()]}}) :: %{
    participating_blocks: [{String.t(), [pid()]}],
    total_map_tasks: integer()
  }
  defp fetch_participating_blocks(state) do
    # The main result of this function is a list of participating blocks and where are they stored.
    # participating_blocks: [{"block_1", [worker_1]}, {"block_2", [worker_2]}]

    # ----------------------------------------------------
    # Dummy data for testing purposes
    Logger.info("Fetching participating blocks")
    workers =state.worker_process_map |> Map.keys()
    participating_blocks =
      1..10 |> Enum.map(fn i ->
        worker_count = Enum.random(1..2)
        worker_list = Enum.take_random(workers, worker_count)
        {"block_#{i}", worker_list}
      end)
    total_map_tasks = Enum.count(participating_blocks)
    Logger.info("Will generate #{total_map_tasks} map tasks")
    # ----------------------------------------------------

    %{state | participating_blocks: participating_blocks, total_map_tasks: total_map_tasks}
  end

  @spec fetch_shuffle_data(%{}) :: map()
  defp fetch_shuffle_data(state) do
    unique_keys_by_storage = fetch_all_unique_keys(state)
    shuffle_data = unique_keys_by_storage
    |> Enum.flat_map(fn {storage_pid, keys} ->
      MapSet.to_list(keys)
      |> Enum.map(fn key -> {key, storage_pid} end)
    end)
    |> Enum.group_by(fn {key, _} -> key end, fn {_, storage_pid} -> storage_pid end)
    |> Enum.map(fn {key, storage_pids} -> {key, storage_pids} end)

    total_reduce_tasks = ceil(Enum.count(shuffle_data) / @max_key_each_reduce_task)

    Logger.info("Will generate #{total_reduce_tasks} reduce tasks")
    %{state | shuffle_data: shuffle_data, total_reduce_tasks: total_reduce_tasks}
  end


  # Task generation
  defp generate_map_tasks(state) do
    map_tasks = state.current_chunk
    |> Enum.map(fn {_block_id, _workers_pids} = input_data  ->
      ComputeTask.new_map(state.job.id, input_data, state.job.map_function, state.job.map_context)
    end)

    Logger.info("Generated #{length(map_tasks)} map tasks")
    %{state | map_tasks: map_tasks}
  end

  defp generate_reduce_tasks(state) do
    reduce_tasks = state.current_chunk
    |> Enum.chunk_every(@max_key_each_reduce_task)
    |> Enum.map(fn keys ->
      ComputeTask.new_reduce(state.job.id, keys, state.job.reduce_function, state.job.reduce_context)
    end)

    Logger.info("Generated #{length(reduce_tasks)} reduce tasks")
    %{state | reduce_tasks: reduce_tasks}
  end

  # Generic task dispatch
  defp dispatch_tasks_with_state(state, tasks_field) do
    Logger.info("Start dispatching #{tasks_field}")
    tasks = Map.get(state, tasks_field)
    {dispatched, failed, _assignments} = dispatch_tasks(tasks, {0, 0, {state.worker_process_map, state.load_balance_runner_tree}})

    phase = if tasks_field == :map_tasks, do: "map", else: "reduce"
    Logger.info("Dispatched #{dispatched} #{phase} tasks, #{failed} failed")
    state
    |> Map.put(tasks_field, [])
  end

  defp wait_for_phase_completion(state, phase, next_phase) do
    total_tasks = Map.get(state, :"total_#{phase}_tasks")

    if total_tasks > 0 do
      %{state | status: :"#{phase}_waiting"}
    else
      Process.send(self(), next_phase, [])
      state
    end
  end

  # Notification helpers
  defp notify_phase_start(state, phase) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.id, phase})
    state
  end

  defp notify_job_start(state) do
    GenServer.cast(ComputeOperation, {:job_status, state.job.id, :starting})
    state
  end

  defp notify_job_completion(state, result) do
    GenServer.cast(ComputeOperation, {:job_completed, state.job.id, result})
    %{state | status: :completed}
  end

  defp notify_job_failure(state, error) do
    GenServer.cast(ComputeOperation, {:job_failed, state.job.id, error})
    %{state | status: :failed}
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

  # Utility functions
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
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
        other -> {:error, :unexpected_response}
      end
    rescue
      error -> {:error, :communication_failed}
    end
  end

  defp process_async_results(results, total_workers) do
    {worker_runner_map, load_tree, failures} =
      Enum.reduce(results, {%{}, :gb_trees.empty(), []}, fn
        {:ok, {worker_pid, storage_pid, task_runner_pid}}, {worker_acc, tree_acc, failure_acc} ->
          new_worker_acc = Map.put(worker_acc, worker_pid, {storage_pid, task_runner_pid})
          new_tree_acc = :gb_trees.insert({0, task_runner_pid}, task_runner_pid, tree_acc)
          {new_worker_acc, new_tree_acc, failure_acc}
        {:exit, reason}, {worker_acc, tree_acc, failure_acc} ->
          {worker_acc, tree_acc, [reason | failure_acc]}
      end)

    successes_count = map_size(worker_runner_map)
    failures_count = length(failures)

    cond do
      successes_count == 0 -> {:error, :no_workers_started}
      failures_count > 0 -> {:ok, worker_runner_map, load_tree}
      true -> {:ok, worker_runner_map, load_tree}
    end
  end

  defp stop_all_task_runners(worker_process_map) do
    Enum.each(worker_process_map, fn {_worker_pid, {storage_pid, task_runner_pid}} ->
      stop_process(task_runner_pid)
      stop_process(storage_pid)
    end)
  end

  defp stop_process(pid) do
    if pid_valid?(pid) do
      try do
        GenServer.stop(pid, :normal, 5000)
      rescue
        _error -> Process.exit(pid, :kill)
      end
    end
  end

  defp pid_valid?(pid) do
    try do
      node = node(pid)
      node == Node.self() or node in Node.list()
    rescue
      ArgumentError -> false
    end
  end

  defp cleanup_after_failed_init(job_id, reason) do
    Logger.info("Cleaning up failed init for job #{job_id}")
    GenServer.cast(ComputeOperation, {:job_failed, job_id, reason})
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Job #{state.job.id} terminated: #{inspect(reason)}")
    stop_all_task_runners(state.worker_process_map)
  end

  # Counter and task dispatch functions
  defp initialize_counters(job_spawned_task_counter_ref) do
    :ets.insert(job_spawned_task_counter_ref, [
      {:completed_map_tasks, 0},
      {:completed_reduce_tasks, 0},
      {:failed_map_tasks, 0},
      {:failed_reduce_tasks, 0}
    ])
  end

  defp get_counter(job_spawned_task_counter_ref, key) do
    [{^key, value}] = :ets.lookup(job_spawned_task_counter_ref, key)
    value
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
      {:ok, {:ok, {storage_pid, keys}}}, acc -> Map.put(acc, storage_pid, keys)
      _, acc -> acc
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
            Enum.map(results, fn {k, v} -> {k, v} end)
          _ ->
            []
        end
      end,
      max_concurrency: length(storage_pids),
      timeout: 15_000
    )
    |> Enum.flat_map(fn
      {:ok, results} -> results  # Now 'results' is just a list, not {:ok, list}
      _ -> []
    end)
  end


  defp dispatch_tasks([], acc), do: acc
  defp dispatch_tasks([task | remaining_tasks], {dispatched, failed, {worker_process_map, load_balance_runner_tree}}) do
    case execute_task(task, worker_process_map, load_balance_runner_tree) do
      {:ok, new_tree} ->
        dispatch_tasks(remaining_tasks, {dispatched + 1, failed, {worker_process_map, new_tree}})
      {:ok, :all_workers_overloaded} ->
        # All workers are overloaded, retrying dispatch
        :timer.sleep(1000) # Small delay
        dispatch_tasks([task | remaining_tasks], {dispatched, failed, {worker_process_map, load_balance_runner_tree}})
      {:error, _reason} ->
        dispatch_tasks(remaining_tasks, {dispatched, failed + 1, {worker_process_map, load_balance_runner_tree}})
    end
  end

  defp execute_task(task, worker_process_map, load_balance_runner_tree) do
    execute_task_with_backpressure(task, worker_process_map, load_balance_runner_tree, load_balance_runner_tree)
  end

  defp execute_task_with_backpressure(task, worker_process_map, original_tree, remaining_tree) do
    cond do
      map_size(worker_process_map) == 0 ->
        {:error, :no_workers_configured}

      :gb_trees.is_empty(remaining_tree) ->
        {:ok, :all_workers_overloaded}

      true ->
        case choose_worker_from_remaining(task, worker_process_map, remaining_tree) do
          {:ok, runner_pid, new_remaining_tree} ->
            case Process.send(runner_pid, {:execute_task, task}, [:nosuspend]) do
              :ok ->
                # Successfully sent task to worker
                case get_runner_from_tree(original_tree, runner_pid) do
                  {{current_count, ^runner_pid}, _value} ->
                    # Remove old entry and insert with incremented count
                    old_tree = :gb_trees.delete({current_count, runner_pid}, original_tree)
                    new_tree = :gb_trees.insert({current_count + 1, runner_pid}, runner_pid, old_tree)
                    {:ok, new_tree}
                  nil ->
                    {:error, :runner_not_found_in_original_tree}
                end
              :nosuspend ->
                # Chosen worker is overloaded, try another one from the remaining tree
                execute_task_with_backpressure(task, worker_process_map, original_tree, new_remaining_tree)
              :noconnect ->
                {:error, :no_connection}
            end
          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # TODO: stil need to add logic data locality with ability to choose worker that is not local to block
  # Currently it just choose the first worker where block is stored.
  defp choose_worker_from_remaining(%{type: :map, input_data: {_block_id, [worker_pid | _]}}, worker_process_map, remaining_tree) do
      case Map.get(worker_process_map, worker_pid) do
        {_storage_pid, runner_pid} ->
          case get_runner_from_tree(remaining_tree, runner_pid) do
            {{current_count, ^runner_pid}, _value} ->
              # Remove from remaining tree and return with updated count
              updated_tree = :gb_trees.delete({current_count, runner_pid}, remaining_tree)
              {:ok, runner_pid, updated_tree}
            nil ->
              {:error, :runner_not_found_in_tree}
          end
        nil ->
          {:error, :worker_not_found}
      end
  end

  defp choose_worker_from_remaining(%{type: :reduce}, _worker_process_map, remaining_tree) do
    case :gb_trees.is_empty(remaining_tree) do
      true ->
        {:error, :no_more_workers}
      false ->
        {{current_count, runner_pid}, _value, new_remaining_tree} = :gb_trees.take_smallest(remaining_tree)
        {:ok, runner_pid, new_remaining_tree}
    end
  end

  defp choose_worker_from_remaining(_task, _worker_process_map, _remaining_tree) do
    {:error, "Invalid task structure"}
  end

  # Keep your existing helper functions
  defp get_runner_from_tree(tree, target_runner_pid) do
    iterator = :gb_trees.iterator(tree)
    find_runner_in_iterator(iterator, target_runner_pid)
  end

  defp find_runner_in_iterator(iterator, target_runner_pid) do
    case :gb_trees.next(iterator) do
      {{_count, runner_pid} = key, value, next_iterator} ->
        if runner_pid == target_runner_pid, do: {key, value}, else: find_runner_in_iterator(next_iterator, target_runner_pid)
      :none -> nil
    end
  end

end
