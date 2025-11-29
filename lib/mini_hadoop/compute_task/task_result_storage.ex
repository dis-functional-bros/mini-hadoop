defmodule MiniHadoop.ComputeTask.TaskResultStorage do
  use GenServer
  require Logger

  @max_concurrent_file_reads 10

  defstruct [
    :job_id,
    :ets_table_ref,
    :storage_dir,
    :unique_keys,
    :map_files,
    :reduce_results
  ]

  # Client API
  def start_link(job_id) do
    GenServer.start_link(__MODULE__, job_id)
  end

  # Server Callbacks
  @impl true
  def init(job_id) do
    storage_dir = Path.join([System.tmp_dir!(), "mini_hadoop", job_id])
    File.mkdir_p!(storage_dir)

    ets_table_ref = :ets.new(nil, [
      :set, :protected,
      {:read_concurrency, true}
    ])

    :ets.insert(ets_table_ref, [
      {:unique_keys, MapSet.new()},
      {:map_files, []},
      {:reduce_results, []},
      {:chunk_counter, 0}
    ])

    state = %__MODULE__{
      job_id: job_id,
      ets_table_ref: ets_table_ref,
      storage_dir: storage_dir,
      unique_keys: MapSet.new(),
      map_files: [],
      reduce_results: []
    }

    Logger.info("Temp storage started for job #{job_id} at #{storage_dir}")
    {:ok, state}
  end

  @impl true
  def handle_call({:store_map_results, key_value_pairs}, _from, state) do
    try do
      batch_keys = Enum.map(key_value_pairs, fn {k, _v} -> k end) |> MapSet.new()
      updated_keys = MapSet.union(state.unique_keys, batch_keys)

      # Each map task writes to its own file
      map_file = store_map_task_results(key_value_pairs, state.storage_dir, state.ets_table_ref)
      updated_map_files = [map_file | state.map_files]  # Prepend for O(1)

      # Update ETS
      :ets.insert(state.ets_table_ref, {:unique_keys, updated_keys})
      :ets.insert(state.ets_table_ref, {:map_files, updated_map_files})

      new_state = %{state |
        unique_keys: updated_keys,
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
  def handle_call({:get_values_of_keys, keys}, _from, state) when is_list(keys) do
    try do
      values_map = fetch_keys_concurrently(keys, state.map_files, state.storage_dir)
      {:reply, {:ok, values_map}, state}
    rescue
      error ->
        Logger.error("Failed to get values for keys #{inspect(keys)}: #{inspect(error)}")
        {:reply, {:error, :read_failed}, state}
    end
  end

  @impl true
  def handle_call({:get_values_of_keys, keys}, _from, state) when is_list(keys) and length(keys) == 1 do
    # Optimized path for single key
    try do
      [key] = keys
      values = fetch_single_key_simple(key, state.map_files, state.storage_dir)
      {:reply, {:ok, %{key => values}}, state}
    rescue
      error ->
        Logger.error("Failed to get value for key #{inspect(hd(keys))}: #{inspect(error)}")
        {:reply, {:error, :read_failed}, state}
    end
  end

  @impl true
  def handle_call(:get_unique_keys, _from, state) do
    [{:unique_keys, keys}] = :ets.lookup(state.ets_table_ref, :unique_keys)
    {:reply, {:ok, keys}, state}
  end

  @impl true
  def handle_call({:store_reduce_results, results}, _from, state) do
    try do
      new_reduce_results = results ++ state.reduce_results
      :ets.insert(state.ets_table_ref, {:reduce_results, new_reduce_results})

      new_state = %{state | reduce_results: new_reduce_results}
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

  # Optimized Private Helper Functions

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
        acc  # Skip failed files, continue with others
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
            Map.update(acc, k, [v], &[v | &1])  # Prepend for O(1)
          else
            acc
          end
        end)

      {:error, reason} ->
        Logger.warning("Failed to read map file #{file}: #{inspect(reason)}",  [])
        %{}  # Return empty results for this file
    end
  end

  defp fetch_single_key_simple(key, map_files, storage_dir) do
    # Simple, reliable approach - read entire files but filter efficiently
    Enum.flat_map(map_files, fn file ->
      file_path = Path.join(storage_dir, file)

      case File.read(file_path) do
        {:ok, binary} ->
          key_value_pairs = :erlang.binary_to_term(binary)
          # Efficiently extract values for this key
          for {k, v} <- key_value_pairs, k == key, do: v
        {:error, _} ->
          []
      end
    end)
  end

  defp store_map_task_results(key_value_pairs, storage_dir, ets_ref) do
    # Each map task gets its own unique file
    task_id = :ets.update_counter(ets_ref, :chunk_counter, 1)
    filename = "map_task_#{task_id}.data"
    file_path = Path.join(storage_dir, filename)

    # FIX: Use term_to_binary to convert Elixir term to binary for storage
    :ok = File.write(file_path, :erlang.term_to_binary(key_value_pairs))

    filename  # Return just the filename for tracking
  end

  defp merge_results_maps(map1, map2) do
    Map.merge(map1, map2, fn _key, values1, values2 ->
      values1 ++ values2  # Combine values from different map tasks
    end)
  end

  defp cleanup_storage(state) do
    File.rm_rf!(state.storage_dir)
    :ets.delete(state.ets_table_ref)
    Logger.info("Cleaned up temp storage for job #{state.job_id}")
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Temp storage terminating for job #{state.job_id}: #{inspect(reason)}")
    cleanup_storage(state)
    :ok
  end
end
