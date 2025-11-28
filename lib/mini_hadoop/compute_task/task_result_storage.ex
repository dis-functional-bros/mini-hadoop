# lib/mini_hadoop/compute_task/task_result_storage.ex
defmodule MiniHadoop.ComputeTask.TaskResultStorage do
  use GenServer
  require Logger

  @default_chunk_size 10_000

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

  @doc """
  Stores map task results efficiently using chunked file storage.
  """
  def store_map_results(storage_pid, key_value_pairs) when is_list(key_value_pairs) do
    GenServer.call(storage_pid, {:store_map_results, key_value_pairs})
  end

  @doc """
  Gets all values for a specific key (streams from disk).
  """
  def get_values(storage_pid, key) do
    GenServer.call(storage_pid, {:get_values, key})
  end

  @doc """
  Gets all unique keys found in map results.
  """
  def get_unique_keys(storage_pid) do
    GenServer.call(storage_pid, :get_unique_keys)
  end

  @doc """
  Stores reduce task result for a key.
  """
  def store_reduce_result(storage_pid, key, value) do
    GenServer.call(storage_pid, {:store_reduce_result, key, value})
  end

  @doc """
  Stores reduce task results as a complete list.
  """
  def store_reduce_results(storage_pid, results) when is_list(results) do
    GenServer.call(storage_pid, {:store_reduce_results, results})
  end

  @doc """
  Gets all reduce results.
  """
  def get_reduce_results(storage_pid) do
    GenServer.call(storage_pid, :get_reduce_results)
  end

  @doc """
  Cleans up temporary storage.
  """
  def cleanup(storage_pid) do
    GenServer.call(storage_pid, :cleanup)
  end

  # Server Callbacks

  @impl true
  def init(job_id) do
    # Create storage directory for this job
    storage_dir = Path.join([System.tmp_dir!(), "mini_hadoop", job_id])
    File.mkdir_p!(storage_dir)

    # Create ETS table for metadata
    ets_table_ref = :ets.new(:task_storage_metadata, [
      :set, :protected, :named_table,
      {:read_concurrency, true}
    ])

    # Initialize ETS with metadata
    :ets.insert(ets_table_ref, [
      {:unique_keys, MapSet.new()},
      {:map_files, []},  # Changed from %{} to list
      {:reduce_results, []},
      {:chunk_counter, 0}
    ])

    state = %__MODULE__{
      job_id: job_id,
      ets_table_ref: ets_table_ref,
      storage_dir: storage_dir,
      unique_keys: MapSet.new(),
      map_files: [],  # Changed from %{} to list
      reduce_results: []
    }

    Logger.info("Temp storage started for job #{job_id} at #{storage_dir}")
    {:ok, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    Logger.info("TaskResultStorage stopping for job #{state.job_id}")
    # Cleanup storage files
    cleanup_storage(state)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_call({:store_map_results, key_value_pairs}, _from, state) do
    try do
      # Extract unique keys from this batch
      batch_keys = Enum.map(key_value_pairs, fn {k, _v} -> k end) |> MapSet.new()
      updated_keys = MapSet.union(state.unique_keys, batch_keys)

      # Store raw key-value pairs in chunk files (no grouping by key)
      chunk_files = store_raw_pairs_to_files(key_value_pairs, state.storage_dir, state.ets_table_ref)

      # Update map files tracking - now just appending to list
      updated_map_files = state.map_files ++ chunk_files

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
  def handle_call({:get_values, key}, _from, state) do
    try do
      # Now we need to group by key on-the-fly when fetching
      values = stream_and_group_values_for_key(key, state.map_files, state.storage_dir)
      {:reply, {:ok, values}, state}
    rescue
      error ->
        Logger.error("Failed to get values for key #{inspect(key)}: #{inspect(error)}")
        {:reply, {:error, :read_failed}, state}
    end
  end

  @impl true
  def handle_call(:get_unique_keys, _from, state) do
    [{:unique_keys, keys}] = :ets.lookup(state.ets_table_ref, :unique_keys)
    {:reply, {:ok, keys}, state}  # Returns {:ok, MapSet.t()}
  end

  @impl true
  def handle_call({:store_reduce_result, key, value}, _from, state) do
    try do
      # Efficient O(1) prepend operation
      new_reduce_results = [{key, value} | state.reduce_results]
      :ets.insert(state.ets_table_ref, {:reduce_results, new_reduce_results})

      new_state = %{state | reduce_results: new_reduce_results}
      {:reply, :ok, new_state}
    rescue
      error ->
        Logger.error("Failed to store reduce result: #{inspect(error)}")
        {:reply, {:error, :storage_failed}, state}
    end
  end

  @impl true
  def handle_call({:store_reduce_results, results}, _from, state) do
    try do
      # Prepend the entire batch (efficient O(1) operation)
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
    # Return as {:ok, list_of_tuples} format
    {:reply, {:ok, state.reduce_results}, state}  # Returns {:ok, [{key, value}]}
  end

  @impl true
  def handle_call(:cleanup, _from, state) do
    # Cleanup files and ETS table
    cleanup_storage(state)
    {:reply, :ok, state}
  end

  # Private helper functions

  defp store_raw_pairs_to_files(key_value_pairs, storage_dir, ets_ref) do
    chunk_id = get_next_chunk_id(ets_ref)
    filename = Path.join(storage_dir, "map_chunk_#{chunk_id}.data")

    # Write raw key-value pairs to file as Erlang term
    :ok = File.write(filename, :erlang.term_to_binary(key_value_pairs))

    # Return list with the new filename
    [filename]
  end

  defp get_next_chunk_id(ets_ref) do
    :ets.update_counter(ets_ref, :chunk_counter, 1)
  end

  defp stream_and_group_values_for_key(key, map_files, storage_dir) do
    map_files
    |> Stream.flat_map(fn file ->
      case File.read(file) do
        {:ok, binary} ->
          # Read all key-value pairs from this chunk
          key_value_pairs = :erlang.binary_to_term(binary)

          # Filter and extract values for the requested key
          key_value_pairs
          |> Enum.filter(fn {k, _v} -> k == key end)
          |> Enum.map(fn {_k, v} -> v end)

        {:error, _} ->
          []
      end
    end)
    |> Enum.to_list()
  end

  defp cleanup_storage(state) do
    # Delete all temporary files
    File.rm_rf!(state.storage_dir)

    # Delete ETS table
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
