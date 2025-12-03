defmodule MiniHadoop.ComputeTask.TaskResultStorage do
  use GenServer
  require Logger

  @max_concurrent_file_reads 10
  @sample_rate 0.2  # 1% sampling

  defstruct [
    :job_id,
    :storage_dir,
    :sorted_unique_keys,     # Keys stored in sorted order for range queries
    :map_files,              # List of map result files
    :reduce_results
  ]

  def start_link(job_id) do
    GenServer.start_link(__MODULE__, job_id)
  end

  # Server Callbacks
  @impl true
  def init(job_id) do
    storage_dir = Path.join([System.tmp_dir!(), "mini_hadoop", job_id])
    File.mkdir_p!(storage_dir)

    state = %__MODULE__{
      job_id: job_id,
      storage_dir: storage_dir,
      sorted_unique_keys: [],
      map_files: [],
      reduce_results: []
    }

    Logger.info("Temp storage started for job #{job_id} at #{storage_dir}")
    {:ok, state}
  end

  @impl true
  def handle_call({:store_map_results, key_value_pairs}, _from, state) do
    try do
      # Extract keys from this batch
      batch_keys = Enum.map(key_value_pairs, fn {k, _v} -> k end)

      # Update sorted keys list (maintain sorted order for range queries)
      new_sorted_keys =
        (state.sorted_unique_keys ++ batch_keys)
        |> Enum.sort()
        |> Enum.uniq()

      Logger.debug("Sorted keys updated: #{inspect(new_sorted_keys)}")

      # Store sorted map results to file
      map_file = store_sorted_map_task_results(key_value_pairs, state.storage_dir, length(state.map_files))
      updated_map_files = [map_file | state.map_files]

      new_state = %{state |
        sorted_unique_keys: new_sorted_keys,
        map_files: updated_map_files
      }

      {:reply, :ok, new_state}
    rescue
      error ->
        Logger.error("Failed to store map results: #{inspect(error)}")
        {:reply, {:error, :storage_failed}, state}
    end
  end

  @impl true
  def handle_call(:get_sample_keys, _from, state) do
    try do
      sorted_keys = state.sorted_unique_keys
      total = length(sorted_keys)

      if total == 0 do
        {:reply, {:ok, []}, state}
      else
        # Calculate sampling parameters
        sample_rate = @sample_rate
        sample_size = max(1, trunc(total * sample_rate))

        # Take evenly distributed sample using arithmetic progression
        step = max(1, trunc(total / sample_size))

        sample_keys =
          0..(sample_size - 1)
          |> Stream.map(fn i -> i * step end)
          |> Stream.filter(&(&1 < total))
          |> Stream.map(fn idx -> Enum.at(sorted_keys, idx) end)
          |> Enum.to_list()

        Logger.debug("Sampled #{length(sample_keys)} of #{total} keys")
        {:reply, {:ok, sample_keys}, state}
      end
    rescue
      error ->
        Logger.error("Failed to get sample keys: #{inspect(error)}")
        {:reply, {:error, :read_failed}, state}
    end
  end

  # Get all keys (sorted)
  @impl true
  def handle_call(:get_all_keys, _from, state) do
    {:reply, {:ok, state.sorted_unique_keys}, state}
  end

  # Get key count
  @impl true
  def handle_call(:get_key_count, _from, state) do
    {:reply, {:ok, length(state.sorted_unique_keys)}, state}
  end

  @impl true
  def handle_call({:get_data_in_range, range_start, range_end}, _from, state) do
    try do
      Logger.info("Fetching data in range #{inspect(range_start)} to #{inspect(range_end)}")

      # Get keys in range with CORRECT semantics: lower exclusive, upper inclusive
      keys_in_range = get_keys_in_range(state.sorted_unique_keys, range_start, range_end)

      Logger.debug("Found #{length(keys_in_range)} keys in range")

      # OPTION 1: Use existing fetch_keys_concurrently (RECOMMENDED)
      values_map = fetch_keys_concurrently(keys_in_range, state.map_files, state.storage_dir)

      Logger.info("Found #{map_size(values_map)} unique keys in range")
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

  defp get_keys_in_range(sorted_keys, range_start, range_end) do
    Enum.filter(sorted_keys, fn key ->
      cond do
        range_start == :first ->
          # Keys ≤ range_end (upper inclusive)
          key <= range_end
        range_end == :last ->
          # Keys > range_start (lower exclusive)
          key > range_start
        true ->
          # range_start < key ≤ range_end (lower exclusive, upper inclusive)
          key > range_start and key <= range_end
      end
    end)
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

  # Private Helper Functions
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

        # Extract only the requested keys from this map task's output
        Enum.reduce(key_value_pairs, %{}, fn {k, v}, acc ->
          if MapSet.member?(keys_set, k) do
            Map.update(acc, k, [v], &[v | &1])
          else
            acc
          end
        end)

      {:error, reason} ->
        Logger.warning("Failed to read map file #{file}: #{inspect(reason)}")
        %{}
    end
  end

  defp fetch_single_key_simple(key, map_files, storage_dir) do
    Enum.flat_map(map_files, fn file ->
      file_path = Path.join(storage_dir, file)

      case File.read(file_path) do
        {:ok, binary} ->
          key_value_pairs = :erlang.binary_to_term(binary)
          for {k, v} <- key_value_pairs, k == key, do: v
        {:error, _} ->
          []
      end
    end)
  end

  # Store map results SORTED by key for efficient range queries
  defp store_sorted_map_task_results(key_value_pairs, storage_dir, task_counter) do
    # Sort key-value pairs by key
    sorted_pairs =
      key_value_pairs
      |> Enum.sort_by(&elem(&1, 0))

    # Group values by key for storage efficiency
    grouped_by_key =
      Enum.group_by(sorted_pairs, &elem(&1, 0), &elem(&1, 1))
      |> Map.to_list()

    # Each map task gets its own unique file
    filename = "map_task_#{task_counter}.sorted.data"
    file_path = Path.join(storage_dir, filename)

    # Store grouped data for efficient retrieval
    :ok = File.write(file_path, :erlang.term_to_binary(grouped_by_key))

    filename
  end

  defp merge_results_maps(map1, map2) do
    Map.merge(map1, map2, fn _key, values1, values2 ->
      values1 ++ values2
    end)
  end

  defp cleanup_storage(state) do
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
