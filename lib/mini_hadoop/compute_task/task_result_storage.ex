defmodule MiniHadoop.ComputeTask.TaskResultStorage do
  use GenServer
  require Logger

  @moduledoc """
  Manages intermediate and final results for MiniHadoop jobs.

  This GenServer handles:
  - Storage of Map task results (written to disk to handle large datasets)
  - Key tracking using ETS for efficient lookups
  - Sampling of keys for partitioning
  - Storage of Reduce task results
  - Serving data to reducers based on key ranges

  The storage strategy uses:
  - In-memory ETS table for storing all unique keys seen so far
  - Disk-based storage for actual value data (to avoid OOM on large jobs)
  - Pre-computed sampling for efficient partition boundary calculation
  """

  @max_concurrent_file_reads 10
  @sample_rate Application.compile_env(:mini_hadoop, :key_sample_rate)
  @max_num_of_sample_keys_per_worker Application.compile_env(
                                       :mini_hadoop,
                                       :max_num_of_sample_keys_per_worker
                                     )

  defstruct [
    :job_id,
    :storage_dir,
    :map_files,
    :reduce_results,
    :keys_ets_ref,
    :precomputed_samples,
    :key_count
  ]

  @doc """
  Starts the result storage for a specific job.

  Creates a temporary directory for file-based storage and initializes
  the ETS table for key tracking.
  """
  def start_link(job_id) do
    GenServer.start_link(__MODULE__, job_id)
  end

  @impl true
  def init(job_id) do
    storage_dir = Path.join([Application.get_env(:mini_hadoop, :temp_path), job_id])
    File.mkdir_p!(storage_dir)

    # Create key tracking ETS table
    # - Using :ordered_set enables range queries for partition handling
    # - :protected ensures only we can write, but others could potentially read if we gave them the ref
    ets_ref =
      :ets.new(nil, [
        :ordered_set,
        :protected,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    state = %__MODULE__{
      job_id: job_id,
      storage_dir: storage_dir,
      keys_ets_ref: ets_ref,
      precomputed_samples: MapSet.new(),
      key_count: 0,
      map_files: [],
      reduce_results: []
    }

    Logger.info("Temp storage started for job #{job_id} at #{storage_dir}")
    {:ok, state}
  end

  # Stores results from a Map task.
  # 1. Identifies new unique keys and adds them to ETS (for tracking availability)
  # 2. Updates reservoir sampling of keys (for later partitioning)
  # 3. Writes the actual data to a disk file to keep memory usage low
  @impl true
  def handle_call({:store_map_results, key_value_pairs}, _from, state) do
    try do
      batch_keys = Enum.map(key_value_pairs, &elem(&1, 0))

      {new_keys_list, _seen} =
        Enum.reduce(batch_keys, {[], %{}}, fn key, {acc, seen} ->
          if Map.has_key?(seen, key) do
            {acc, seen}
          else
            case :ets.lookup(state.keys_ets_ref, key) do
              [] ->
                {[key | acc], Map.put(seen, key, true)}

              [_] ->
                {acc, Map.put(seen, key, true)}
            end
          end
        end)

      if new_keys_list != [] do
        Enum.each(new_keys_list, fn key ->
          :ets.insert(state.keys_ets_ref, {key})
        end)
      end

      new_samples =
        if new_keys_list == [] do
          state.precomputed_samples || MapSet.new()
        else
          update_samples(
            state.precomputed_samples || MapSet.new(),
            new_keys_list,
            @sample_rate
          )
        end

      map_file =
        store_map_task_results(key_value_pairs, state.storage_dir, length(state.map_files))

      new_state = %{
        state
        | precomputed_samples: new_samples,
          key_count: state.key_count + length(new_keys_list),
          map_files: [map_file | state.map_files]
      }

      {:reply, :ok, new_state}
    rescue
      error ->
        Logger.error("Failed to store map results: #{inspect(error)}")
        {:reply, {:error, :storage_failed}, state}
    end
  end

  defp update_samples(existing_samples, new_keys, sample_rate) do
    current_size = MapSet.size(existing_samples)

    if current_size >= @max_num_of_sample_keys_per_worker do
      existing_samples
    else
      remaining_capacity = @max_num_of_sample_keys_per_worker - current_size

      Enum.reduce(new_keys, {existing_samples, remaining_capacity}, fn key,
                                                                       {samples, remaining} ->
        if remaining > 0 and :rand.uniform() <= sample_rate do
          {MapSet.put(samples, key), remaining - 1}
        else
          {samples, remaining}
        end
      end)
      |> elem(0)
    end
  end

  # Returns the current set of sampled keys.
  # Used by the Master to calculate partition boundaries for Reducers
  @impl true
  def handle_call(:get_sample_keys, _from, state) do
    samples =
      case state.precomputed_samples do
        nil -> []
        mapset -> MapSet.to_list(mapset) |> Enum.sort()
      end

    {:reply, {:ok, samples}, state}
  end

  # Retrieves all values associated with keys falling within the specified range.
  # This is the core data shuffling mechanism - pulling data for a specific reducer partition.
  # 1. Queries ETS to find which keys exist in the range
  # 2. Scans all stored map files concurrently to extract values for those keys
  @impl true
  def handle_call({:get_data_in_range, range_start, range_end}, _from, state) do
    try do
      keys_in_range = get_keys_in_range_from_ets(state.keys_ets_ref, range_start, range_end)
      values_map = fetch_keys_concurrently(keys_in_range, state.map_files, state.storage_dir)
      {:reply, {:ok, values_map}, state}
    rescue
      error ->
        Logger.error("""
        Failed to get data in range:
        Range: #{inspect(range_start)} to #{inspect(range_end)}
        Error: #{inspect(error)}
        """)

        {:reply, {:error, :range_read_failed}, state}
    end
  end

  # Stores the output of a Reduce task (final job results).
  # These are currently kept in memory in the state list.
  @impl true
  def handle_call({:store_reduce_results, results}, _from, state) do
    try do
      new_state = %{state | reduce_results: results ++ state.reduce_results}
      {:reply, :ok, new_state}
    rescue
      error ->
        Logger.error("Failed to store reduce results: #{inspect(error)}")
        {:reply, {:error, :storage_failed}, state}
    end
  end

  # Returns all accumulated reduce results.
  @impl true
  def handle_call(:get_reduce_results, _from, state) do
    {:reply, {:ok, state.reduce_results}, state}
  end

  # Stops the storage process, triggering cleanup via terminate/2.
  @impl true
  def handle_call(:stop, _from, state) do
    Logger.info("TaskResultStorage stopping for job #{state.job_id}")
    cleanup_storage(state)
    {:stop, :normal, :ok, state}
  end

  defp get_keys_in_range_from_ets(ets_ref, range_start, range_end) do
    match_spec =
      case {range_start, range_end} do
        {:first, :last} ->
          [{{:"$1"}, [], [:"$1"]}]

        {:first, end_key} ->
          [{{:"$1"}, [{:"=<", :"$1", end_key}], [:"$1"]}]

        {start_key, :last} ->
          [{{:"$1"}, [{:>, :"$1", start_key}], [:"$1"]}]

        {start_key, end_key} ->
          [
            {{:"$1"},
             [
               {:andalso, {:>, :"$1", start_key}, {:"=<", :"$1", end_key}}
             ], [:"$1"]}
          ]
      end

    :ets.select(ets_ref, match_spec)
  end

  defp fetch_keys_concurrently(keys, map_files, storage_dir) do
    keys_set = MapSet.new(keys)

    map_files
    |> Task.async_stream(
      fn file ->
        process_single_map_file(file, keys_set, storage_dir)
      end,
      max_concurrency: @max_concurrent_file_reads,
      timeout: 30_000
    )
    |> Enum.reduce(%{}, fn
      {:ok, file_results}, acc ->
        merge_results_maps(acc, file_results)

      {:exit, _reason}, acc ->
        acc
    end)
  end

  defp process_single_map_file(file, keys_set, storage_dir) do
    file_path = Path.join(storage_dir, file)

    case File.read(file_path) do
      {:ok, binary} ->
        key_value_pairs = :erlang.binary_to_term(binary)

        Enum.reduce(key_value_pairs, %{}, fn {k, v}, acc ->
          if MapSet.member?(keys_set, k) do
            Map.put(acc, k, v)
          else
            acc
          end
        end)

      {:error, reason} ->
        Logger.warning("Failed to read map file #{file}: #{inspect(reason)}")
        %{}
    end
  end

  defp merge_results_maps(map1, map2) do
    Map.merge(map1, map2, fn _key, values1, values2 ->
      values1 ++ values2
    end)
  end

  defp store_map_task_results(key_value_pairs, storage_dir, task_counter) do
    grouped_by_key =
      Enum.reduce(key_value_pairs, %{}, fn {key, value}, acc ->
        Map.update(acc, key, [value], &[value | &1])
      end)
      |> Map.to_list()

    filename = "map_task_#{task_counter}.data"
    file_path = Path.join(storage_dir, filename)

    :ok = File.write(file_path, :erlang.term_to_binary(grouped_by_key))

    filename
  end

  defp cleanup_storage(state) do
    if state.keys_ets_ref do
      :ets.delete(state.keys_ets_ref)
    end

    File.rm_rf!(state.storage_dir)

    Logger.info("Cleaned up temp storage for job #{state.job_id}")
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Temp storage terminating for job #{state.job_id}: #{inspect(reason)}")
    cleanup_storage(state)
    :ok
  end
end
