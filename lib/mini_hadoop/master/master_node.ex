defmodule MiniHadoop.Master.MasterNode do
  use GenServer
  require Logger

  def start_link(args \\ %{}) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def pop_smallest do
    GenServer.call(__MODULE__, :pop_smallest, :infinity)
  end

  def update_tree(updated_worker_state) do
    GenServer.call(__MODULE__, {:update_tree, updated_worker_state})
  end

  def lookup_block_owner(block_id) when is_binary(block_id) do
    GenServer.call(__MODULE__, {:lookup_block_owner, block_id})
  end

  def find_blocks_for_filename(filename) when is_binary(filename) do
    GenServer.call(__MODULE__, {:find_blocks_for_filename, filename})
  end

  def register_file_blocks(filename, block_ids) when is_binary(filename) and is_list(block_ids) do
    GenServer.call(__MODULE__, {:register_file_blocks, filename, block_ids})
  end

  def register_block_worker(block_id, worker_info) when is_binary(block_id) do
    GenServer.call(__MODULE__, {:register_block_worker, block_id, worker_info})
  end

  def unregister_file_blocks(filename) when is_binary(filename) do
    GenServer.call(__MODULE__, {:unregister_file_blocks, filename})
  end

  def unregister_block_worker(block_id) when is_binary(block_id) do
    GenServer.call(__MODULE__, {:unregister_block_worker, block_id})
  end

  # init untuk state
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
       filename_to_blocks: %{},
       block_to_worker: %{}
     }}
  end

  @impl true
  def handle_call({:register_worker, worker_state}, _from, state) do
    new_workers =
      Map.put(state.workers, worker_state.hostname, %{
        worker_state
        | last_heartbeat: :os.system_time(:millisecond)
      })

    new_tree = rebuild_tree(new_workers)
    {:reply, :ok, %{state | workers: new_workers, tree: new_tree}}
  end

  @impl true
  def handle_call(:list_worker, _from, state) do
    {:reply, state.workers, state}
  end

  @impl true
  def handle_call({:lookup_block_owner, block_id}, _from, state) do
    case Map.get(state.block_to_worker, block_id) do
      nil ->
        {:reply, {:error, :block_not_found}, state}

      worker_pid ->
        # Get the latest worker info from workers map
        worker_info =
          state.workers
          |> Map.values()
          |> Enum.find(fn info -> info.pid == worker_pid end)

        case worker_info do
          nil -> {:reply, {:error, :worker_not_found}, state}
          info -> {:reply, {:ok, info}, state}
        end
    end
  end

  @impl true
  def handle_call({:find_blocks_for_filename, filename}, _from, state) do
    case Map.get(state.filename_to_blocks, filename) do
      nil ->
        {:reply, {:error, :file_not_found}, state}

      block_ids ->
        # Get block owners and sort by index
        blocks_with_owners =
          block_ids
          |> Enum.map(fn block_id ->
            # Extract block index from block_id (format: "filename_block_0")
            block_prefix = "#{filename}_block_"
            index_str = String.replace(block_id, block_prefix, "")

            case Integer.parse(index_str) do
              {index, _} ->
                worker_pid = Map.get(state.block_to_worker, block_id)

                case worker_pid do
                  nil ->
                    nil

                  _ ->
                    # Get the latest worker info from workers map
                    worker_info =
                      state.workers
                      |> Map.values()
                      |> Enum.find(fn info -> info.pid == worker_pid end)

                    case worker_info do
                      nil -> nil
                      info -> {index, block_id, info}
                    end
                end

              :error ->
                nil
            end
          end)
          |> Enum.filter(&(&1 != nil))
          |> Enum.sort_by(fn {index, _block_id, _worker_info} -> index end)

        {:reply, {:ok, blocks_with_owners}, state}
    end
  end

  @impl true
  def handle_call({:register_file_blocks, filename, block_ids}, _from, state) do
    new_filename_to_blocks = Map.put(state.filename_to_blocks, filename, block_ids)
    {:reply, :ok, %{state | filename_to_blocks: new_filename_to_blocks}}
  end

  @impl true
  def handle_call({:register_block_worker, block_id, worker_info}, _from, state) do
    new_block_to_worker = Map.put(state.block_to_worker, block_id, worker_info.pid)
    {:reply, :ok, %{state | block_to_worker: new_block_to_worker}}
  end

  @impl true
  def handle_call({:unregister_file_blocks, filename}, _from, state) do
    IO.inspect(state.filename_to_blocks, label: "state.filename_to_blocks")
    new_filename_to_blocks = Map.delete(state.filename_to_blocks, filename)
    {:reply, :ok, %{state | filename_to_blocks: new_filename_to_blocks}}
  end

  @impl true
  def handle_call({:unregister_block_worker, block_id}, _from, state) do
    IO.inspect(state.block_to_worker, label: "state.block_to_worker")
    new_block_to_worker = Map.delete(state.block_to_worker, block_id)
    {:reply, :ok, %{state | block_to_worker: new_block_to_worker}}
  end

  @impl true
  def handle_call(:pop_smallest, from, state) do
    cond do
      map_size(state.workers) == 0 ->
        {:reply, {:error, :no_worker_registered}, state}

      :gb_trees.is_empty(state.tree) ->
        new_wait_queue = :queue.in(from, state.wait_queue)

        # balas nanti, tidur di Beam VM
        {:noreply, %{state | wait_queue: new_wait_queue}}

      true ->
        {_key, worker, new_tree} = :gb_trees.take_smallest(state.tree)
        {:reply, {:ok, worker}, %{state | tree: new_tree}}
    end
  end

  @impl true
  def handle_call({:update_tree, updated_worker_state}, _from, state) do
    # First, remove the old worker entry from the tree (if it exists)
    old_worker = Map.get(state.workers, updated_worker_state.hostname)

    new_tree =
      if old_worker do
        # Remove the old entry using the old block count
        old_key = {length(old_worker.blocks), old_worker.pid}

        case :gb_trees.is_defined(old_key, state.tree) do
          true -> :gb_trees.delete(old_key, state.tree)
          false -> state.tree
        end
      else
        state.tree
      end

    # Update the workers map
    new_map_workers =
      Map.update!(state.workers, updated_worker_state.hostname, fn worker ->
        Map.put(worker, :blocks, updated_worker_state.blocks)
      end)

    # Insert the updated worker with new block count
    new_tree =
      :gb_trees.insert(
        {length(updated_worker_state.blocks), updated_worker_state.pid},
        updated_worker_state,
        new_tree
      )

    state = %{state | tree: new_tree, workers: new_map_workers}

    case :queue.out(state.wait_queue) do
      {:empty, _queue} ->
        {:reply, :ok, state}

      {{:value, from}, new_queue} ->
        GenServer.reply(from, {:ok, updated_worker_state})

        # Take the smallest (worker with least blocks)
        {_key, _worker, new_tree2} = :gb_trees.take_smallest(state.tree)

        {:reply, :ok, %{state | tree: new_tree2, wait_queue: new_queue}}
    end
  end

  @impl true
  def handle_cast({:receive_heartbeat, worker_hostname}, state) do
    if Map.has_key?(state.workers, worker_hostname) do
      # Syntax: Map.update!(map, key, fun). Hanya berhasil jika key ada.
      updated_workers =
        Map.update!(state.workers, worker_hostname, fn info ->
          %{info | last_heartbeat: :os.system_time(:millisecond)}
        end)

      Logger.debug("Received heartbeat from #{worker_hostname}")
      {:noreply, %{state | workers: updated_workers}}
    else
      Logger.warning("Received heartbeat from unknown worker #{worker_hostname}")
      {:noreply, state}
    end
  end

  def rebuild_tree(workers_map) do
    Enum.reduce(workers_map, :gb_trees.empty(), fn {_id, info}, acc ->
      key = {length(info.blocks), info.pid}
      :gb_trees.insert(key, info, acc)
    end)
  end
end
