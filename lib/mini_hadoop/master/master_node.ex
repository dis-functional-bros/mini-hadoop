defmodule MiniHadoop.Master.MasterNode do
  use GenServer
  require Logger

  alias MiniHadoop.Master.ComputeOperation

  def start_link(args \\ %{}) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  def pop_smallest(block_id) do
    GenServer.call(__MODULE__, {:pop_smallest, block_id}, :infinity)
  end

  def update_tree(updated_worker_state) do
    GenServer.call(__MODULE__, {:update_tree, updated_worker_state})
  end

  def filename_exists(filename) when is_binary(filename) do
    GenServer.call(__MODULE__, {:filename_exists, filename})
  end

  def fetch_blocks_by_filenames(filenames) when is_list(filenames) do
    GenServer.call(__MODULE__, {:fetch_blocks_by_filenames, filenames})
  end

  def get_blocks_assingment_for_file(filename) when is_binary(filename) do
    GenServer.call(__MODULE__, {:get_blocks_assingment_for_file, filename})
  end

  def register_file_blocks(filename, block_ids) when is_binary(filename) and is_list(block_ids) do
    GenServer.call(__MODULE__, {:register_file_blocks, filename, block_ids})
  end

  def unregister_file_blocks(filename) when is_binary(filename) do
    GenServer.call(__MODULE__, {:unregister_file_blocks, filename})
  end

  def rebuild_tree_after_deletion do
    GenServer.call(__MODULE__, {:rebuild_tree_after_deletion})
  end

  # ========== INIT ==========
  @impl true
  def init(_) do
    {:ok,
     %{
       pid: self(),
       hostname: Node.self(),
       status: :idle,
       running_task: nil,
       workers: %{},
       tree: nil,
       wait_queue: :queue.new(),
       # Store mapping of filename to block IDs, {filename => [block_id, block_id, ...]}
       filename_to_blocks: %{},
       # Store mapping of block ID assignments to worker {worker_pid => {block_id=>true, block_id => true}}
       worker_to_block_mapping: %{},
       # Store mapping of worker information to block IDs (reverse mapping of block assignments) {block_id => [worker_pid, worker_pid, ...]}
       block_to_worker_mapping: %{}
     }}
  end

  # ========== HANDLE_CALL FUNCTIONS (GROUPED) ==========

  @impl true
  def handle_call({:register_worker, worker_state}, _from, state) do
    new_workers =
      Map.put(state.workers, worker_state.hostname, %{
        worker_state
        | last_heartbeat: :os.system_time(:millisecond)
      })

    ComputeOperation.register_worker(worker_state.pid)

    Process.monitor(worker_state.pid)

    new_tree = rebuild_tree(new_workers)
    {:reply, :ok, %{state | workers: new_workers, tree: new_tree}}
  end

  @impl true
  def handle_call({:filename_exists, filename}, _from, state) do
    {:reply, Map.has_key?(state.filename_to_blocks, filename), state}
  end

  @impl true
  def handle_call({:fetch_blocks_by_filenames, filenames}, _from, %{
        filename_to_blocks: filename_to_blocks,
        block_to_worker_mapping: block_to_worker_mapping
      } = state) do

    result =
      filenames
      |> Enum.flat_map(&Map.get(filename_to_blocks, &1, []))
      |> Enum.uniq()
      |> Enum.map(fn block_id ->
        workers = Map.get(block_to_worker_mapping, block_id, [])
        {block_id, workers}
      end)

    {:reply, result, state}
  end

  @impl true
  def handle_call({:get_blocks_assingment_for_file, filename}, _from, state) do
    case Map.get(state.filename_to_blocks, filename) do
      nil ->
        {:reply, {:error, :file_not_found}, state}

      block_ids ->
        blocks_with_owners =
          block_ids
          |> Enum.map(fn block_id ->
            index_str = String.replace(block_id, "#{filename}_block_", "")

            case Integer.parse(index_str) do
              {index, _} ->
                worker_pids = Map.get(state.block_to_worker_mapping, block_id, [])
                {index, block_id, worker_pids}
              :error ->
                nil
            end
          end)
          |> Enum.filter(&(&1 != nil))

        {:reply, {:ok, blocks_with_owners}, state}
    end
  end

  @impl true
  def handle_call({:register_file_blocks, filename, block_ids}, _from, state) do
    new_filename_to_blocks = Map.put(state.filename_to_blocks, filename, block_ids)
    {:reply, :ok, %{state | filename_to_blocks: new_filename_to_blocks}}
  end

  @impl true
  def handle_call({:unregister_file_blocks, filename}, _from, state) do
    new_filename_to_blocks = Map.delete(state.filename_to_blocks, filename)
    {:reply, :ok, %{state | filename_to_blocks: new_filename_to_blocks}}
  end

  @impl true
  def handle_call({:pop_smallest, block_id}, from, state) do
    cond do
      map_size(state.workers) == 0 ->
        {:reply, {:error, :no_worker_registered}, state}

      :gb_trees.is_empty(state.tree) ->
        new_wait_queue = :queue.in({from, block_id}, state.wait_queue)
        IO.puts("No suitable worker, enter queue")
        {:noreply, %{state | wait_queue: new_wait_queue}}

      true ->
        # Get list of worker pids that already have this block
        exclude_pids = Map.get(state.block_to_worker_mapping, block_id, [])
        case find_smallest_excluding(state.tree, exclude_pids) do
          {:ok, {_block_count, worker_pid} = key, _hostname} ->
            new_tree = :gb_trees.delete(key, state.tree)
            {:reply, {:ok, worker_pid}, %{state | tree: new_tree}}

          :not_found ->
            # No suitable worker found, add to queue with block_id
            new_wait_queue = :queue.in({from, block_id}, state.wait_queue)
            {:noreply, %{state | wait_queue: new_wait_queue}}
        end
    end
  end

  @impl true
  def handle_call({:update_tree, worker_state}, _from, state) do
    {worker, initial_block_map, initial_worker_list} = case worker_state do
      {:store, worker} ->
        {worker, %{worker.changed_block => true}, [worker.pid]}
      {:delete, worker} ->
        {worker, %{}, []}
    end

    new_worker_to_block_mapping = Map.update(
      state.worker_to_block_mapping, worker.pid, initial_block_map,
      fn existing_block_map ->
        case worker_state do
          {:store, _} -> Map.put(existing_block_map, worker.changed_block, true)
          {:delete, _} -> Map.delete(existing_block_map, worker.changed_block)
        end
      end
    )

    new_block_to_worker_mapping = Map.update(
      state.block_to_worker_mapping, worker.changed_block, initial_worker_list,
      fn existing_workers ->
        case worker_state do
          {:store, _} -> [worker.pid | existing_workers]
          {:delete, _} -> List.delete(existing_workers, worker.pid)
        end
      end
    )

    # Clean up empty mappings
    new_block_to_worker_mapping =
      new_block_to_worker_mapping
      |> Enum.reject(fn {_block, workers} -> workers == [] end)
      |> Map.new()

    new_tree = case worker_state do
      {:store, _} -> :gb_trees.insert({worker.blocks_count, worker.pid}, worker.hostname, state.tree)
      {:delete, _} -> state.tree
    end

    new_workers = Map.put(state.workers, worker.hostname, Map.delete(worker, :changed_block))

    new_state = %{state |
      tree: new_tree,
      workers: new_workers,
      block_to_worker_mapping: new_block_to_worker_mapping,
      worker_to_block_mapping: new_worker_to_block_mapping
    }

    case worker_state do
      {:store, _} ->
        # Process queue ASYNCHRONOUSLY to avoid deadlocks
        if not :queue.is_empty(new_state.wait_queue) do
          GenServer.cast(self(), :process_queue)
        end
        {:reply, :ok, new_state}
      {:delete, _} ->
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:rebuild_tree_after_deletion}, _from, state) do
    {:reply, :ok, %{state | tree: rebuild_tree(state.workers)}}
  end

  @impl true
  def handle_call(:list_worker, _from, state) do
    {:reply, state.workers, state}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  # implementation for re-replication of blocks
  @impl true
  def handle_info({:DOWN, worker_pid, _, _, _}, state) do
    Logger.warning("Worker #{inspect(worker_pid)} went down, starting re-replication process")

    # Get all blocks that were stored on the failed worker
    block_ids = Map.get(state.worker_to_block_mapping, worker_pid, MapSet.new()) |> MapSet.to_list()

    # for every under-replicated block
    replication_results =
      Task.async_stream(block_ids, fn block_id ->
        # Get current workers that have this block (excluding the dead one)
        current_replicas = Map.get(state.block_to_worker_mapping, block_id, [])
                          |> List.delete(worker_pid)

        cond do
          # No replicas left
          length(current_replicas) == 0 ->
            Logger.error("Block #{block_id} has NO remaining replicas - DATA LOST")
            {:error, :no_replicas, block_id}

          # Still have replicas
          true ->
            case find_smallest_excluding(state.tree, current_replicas) do
              {:ok, {_count, target_worker_pid}, _hostname} ->

                source_worker_pid = hd(current_replicas)

                case replicate_block_between_workers(source_worker_pid, target_worker_pid, block_id) do
                  :ok ->
                    {:ok, block_id, target_worker_pid}
                  {:error, reason} ->
                    Logger.error("Failed to re-replicate block #{block_id}: #{inspect(reason)}")
                    {:error, reason, block_id}
                end

              :not_found ->
                Logger.warning("No available worker to re-replicate block #{block_id}")
                {:error, :no_target_worker, block_id}
            end
        end
      end,
      max_concurrency: 5,
      timeout: 30_000
      )
      |> Enum.to_list()

    # Update block_to_worker_mapping
    new_block_to_worker_mapping =
      Enum.reduce(replication_results, state.block_to_worker_mapping, fn result, acc ->
        case result do
          {:ok, {:ok, block_id, new_worker_pid}} ->
            Map.update(acc, block_id, [new_worker_pid], fn existing ->
              existing
              |> List.delete(worker_pid)
              |> Kernel.++([new_worker_pid])
              |> Enum.uniq()
            end)

          {:ok, {:error, _reason, block_id}} ->
            Map.update(acc, block_id, [], fn existing ->
              List.delete(existing, worker_pid)
            end)

          _ ->
            acc
        end
      end)
      |> Enum.reject(fn {_block_id, workers} -> workers == [] end)
      |> Map.new()

    # remove dead worker from all mappings
    dead_worker_hostname =
      Enum.find_value(state.workers, fn {hostname, info} ->
        if info.pid == worker_pid, do: hostname
      end)

    new_workers = if dead_worker_hostname do
      Map.delete(state.workers, dead_worker_hostname)
    else
      state.workers
    end

    new_worker_to_block_mapping = Map.delete(state.worker_to_block_mapping, worker_pid)

    new_tree = rebuild_tree(new_workers)

    new_state = %{state |
      workers: new_workers,
      worker_to_block_mapping: new_worker_to_block_mapping,
      block_to_worker_mapping: new_block_to_worker_mapping,
      tree: new_tree
    }

    {:noreply, new_state}
  end

  # Helper function to replicate a block from source worker to target worker
  defp replicate_block_between_workers(source_pid, target_pid, block_id) do
    try do
      case GenServer.call(source_pid, {:retrieve_block, block_id}, 30_000) do
        {:ok, block_data} ->
          case GenServer.call(target_pid, {:store_block, block_id, block_data}, 30_000) do
            {:store, _worker_state} ->
              :ok
            other ->
              {:error, {:unexpected_store_response, other}}
          end

        {:error, reason} ->
          {:error, {:retrieve_failed, reason}}

        other ->
          {:error, {:unexpected_retrieve_response, other}}
      end
    rescue
      error -> {:error, {:exception, error}}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  # ========== HANDLE_CAST FUNCTIONS (GROUPED) ==========

  @impl true
  def handle_cast({:receive_heartbeat, worker_hostname}, state) do
    if Map.has_key?(state.workers, worker_hostname) do
      # Syntax: Map.update!(map, key, fun). Hanya berhasil jika key ada.
      updated_workers =
        Map.update!(state.workers, worker_hostname, fn info ->
          %{info | last_heartbeat: :os.system_time(:millisecond)}
        end)

      {:noreply, %{state | workers: updated_workers}}
    else
      Logger.warning("Received heartbeat from unknown worker #{worker_hostname}")
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast(:process_queue, state) do
    case :queue.out(state.wait_queue) do
      {:empty, _queue} ->
        {:noreply, state}

      {{:value, {waiting_from, block_id}}, new_queue} ->
        exclude_pids = Map.get(state.block_to_worker_mapping, block_id, [])

        case find_smallest_excluding(state.tree, exclude_pids) do
          {:ok, {_block_count, worker_pid} = key, _hostname} ->
            new_tree_after_removal = :gb_trees.delete(key, state.tree)
            GenServer.reply(waiting_from, {:ok, worker_pid})

            GenServer.cast(self(), :process_queue)
            {:noreply, %{state | tree: new_tree_after_removal, wait_queue: new_queue}}

          :not_found ->
            {:noreply, state}
        end
    end
  end

  # ========== PRIVATE FUNCTIONS ==========

  defp rebuild_tree(workers_map) do
    Enum.reduce(workers_map, :gb_trees.empty(), fn {_id, info}, acc ->
      key = {info.blocks_count, info.pid}
      :gb_trees.insert(key, info.hostname, acc)
    end)
  end

  defp find_smallest_excluding(tree, exclude_pids) do
    iterator = :gb_trees.iterator(tree)
    find_in_iterator_excluding(iterator, exclude_pids)
  end

  defp find_in_iterator_excluding(iterator, exclude_pids) do
    case :gb_trees.next(iterator) do
      {{_block_count, worker_pid}=key, hostname, next_iterator} ->
        if worker_pid in exclude_pids do
          find_in_iterator_excluding(next_iterator, exclude_pids)
        else
          {:ok, key, hostname}
        end
      :none -> :not_found
    end
  end
end
