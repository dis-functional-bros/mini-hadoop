defmodule MiniHadoop.ComputeTask.TaskResultStorage do
  use GenServer
  require Logger

  @max_concurrent_file_reads 10
  @sample_rate 0.2  # 20% sampling

  defstruct [
    :job_id,
    :storage_dir,
    :map_files,
    :reduce_results,
    # Unnamed ETS table reference
    :keys_ets_ref,          # Reference to unnamed ETS table
    :precomputed_samples,   # Precomputed sample keys
    :key_count              # Total unique key count
  ]

  def start_link(job_id) do
    GenServer.start_link(__MODULE__, job_id)
  end

  # Server Callbacks
  @impl true
  def init(job_id) do
    storage_dir = Path.join([System.tmp_dir!(), "mini_hadoop", job_id])
    File.mkdir_p!(storage_dir)

    # Create UNNAMED ETS table (automatically GC'd when process dies)
    ets_ref = :ets.new(nil, [
      :ordered_set,           # Automatically maintains sort order
      :protected,             # Only owner process can write
      {:write_concurrency, true},
      {:read_concurrency, true}          # No heir - table dies with process
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

  @impl true
  def handle_call({:store_map_results, key_value_pairs}, _from, state) do
    try do
      # Extract keys - single pass
      batch_keys = Enum.map(key_value_pairs, &elem(&1, 0))

      # Find new unique keys in one pass
      {new_keys_list, _seen} =
        Enum.reduce(batch_keys, {[], %{}}, fn key, {acc, seen} ->
          # Skip if already processed in this batch
          if Map.has_key?(seen, key) do
            {acc, seen}
          else
            # Check if key exists in ETS
            case :ets.lookup(state.keys_ets_ref, key) do
              [] ->
                # New key - add to results
                {[key | acc], Map.put(seen, key, true)}
              [_] ->
                # Existing key - just mark as seen
                {acc, Map.put(seen, key, true)}
            end
          end
        end)

      # Insert new keys into ETS
      if new_keys_list != [] do
        Enum.each(new_keys_list, fn key ->
          :ets.insert(state.keys_ets_ref, {key})
        end)
      end

      # SIMPLE PROBABILITY SAMPLING with reasonable limit
      new_samples =
        if new_keys_list == [] do
          # No new keys, samples unchanged
          state.precomputed_samples || MapSet.new()
        else
          update_samples(
            state.precomputed_samples || MapSet.new(),
            new_keys_list,
            @sample_rate
          )
        end

      # Store data (unsorted)
      map_file = store_map_task_results(key_value_pairs, state.storage_dir, length(state.map_files))

      new_state = %{state |
        precomputed_samples: new_samples,
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
    # Limit: Don't let samples grow beyond reasonable size
    max_samples = 1000  # Maximum samples to keep

    current_size = MapSet.size(existing_samples)

    if current_size >= max_samples do
      # Already at max - don't add more
      existing_samples
    else
      # Calculate how many more we can add
      remaining_capacity = max_samples - current_size

      # Sample new keys with probability, but respect capacity limit
      Enum.reduce(new_keys, {existing_samples, remaining_capacity},
        fn key, {samples, remaining} ->
          if remaining > 0 and :rand.uniform() <= sample_rate do
            {MapSet.put(samples, key), remaining - 1}
          else
            {samples, remaining}
          end
        end)
      |> elem(0)
    end
  end

  @impl true
  def handle_call(:get_sample_keys, _from, state) do
    # Convert MapSet to sorted list for response
    samples =
      case state.precomputed_samples do
        nil -> []
        mapset -> MapSet.to_list(mapset) |> Enum.sort()
      end

    {:reply, {:ok, samples}, state}
  end

  # @impl true
  # def handle_call(:get_all_keys, _from, state) do
  #   # Get all keys from ETS (already sorted)
  #   keys = :ets.match(state.keys_ets_ref, {'$1'}) |> List.flatten()
  #   {:reply, {:ok, keys}, state}
  # end

  # @impl true
  # def handle_call(:get_key_count, _from, state) do
  #   {:reply, {:ok, state.key_count}, state}
  # end

  @impl true
  def handle_call({:get_data_in_range, range_start, range_end}, _from, state) do
    try do

      # Get keys in range from ETS using match specs
      keys_in_range = get_keys_in_range_from_ets(state.keys_ets_ref, range_start, range_end)

      # Fetch values for those keys
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

  @impl true
  def handle_call(:get_reduce_results, _from, state) do
    {:reply, {:ok, state.reduce_results}, state}
  end

  @impl true
  def handle_call(:cleanup, _from, state) do
    cleanup_storage(state)
    {:reply, :ok, state}
  end

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
          # All keys - match everything
          [{{:'$1'}, [], [:'$1']}]

        {:first, end_key} ->
          # Keys ≤ end_key (upper inclusive)
          [{{:'$1'}, [{:'=<', :'$1', end_key}], [:'$1']}]

        {start_key, :last} ->
          # Keys > start_key (lower exclusive)
          [{{:'$1'}, [{:'>', :'$1', start_key}], [:'$1']}]

        {start_key, end_key} ->
          # start_key < key ≤ end_key (lower exclusive, upper inclusive)
          # CORRECT: Use :andalso to combine conditions
          [{{:'$1'}, [
            {:andalso,
              {:'>', :'$1', start_key},
              {:'=<', :'$1', end_key}
            }
          ], [:'$1']}]
      end

    :ets.select(ets_ref, match_spec)
  end

  # File operations
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
        # This returns a list of {key, [values]} pairs
        key_value_pairs = :erlang.binary_to_term(binary)

        # Convert to map for efficient lookup
        Enum.reduce(key_value_pairs, %{}, fn {k, v}, acc ->
          if MapSet.member?(keys_set, k) do
            # v is already a list from storage
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
      # values1 and values2 are both lists, just concatenate them
      values1 ++ values2
    end)
  end

  defp store_map_task_results(key_value_pairs, storage_dir, task_counter) do
    # Store as-is, no sorting needed
    # Group by key for storage efficiency (still O(n) but no sort)
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
    # Clean up ETS table (unnamed, but we delete it explicitly)
    if state.keys_ets_ref do
      :ets.delete(state.keys_ets_ref)
    end

    # Clean up storage directory
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
