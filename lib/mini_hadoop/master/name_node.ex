defmodule MiniHadoop.Master.NameNode do
  @moduledoc """
  NameNode manages file system metadata and block locations.
  Handles file operations: store, read, delete, list
  """
  use GenServer
  require Logger

  defstruct [
    :file_registry,      # Map of filename -> %{blocks: [block_id], size: int, created_at: datetime}
    :block_locations,    # Map of block_id -> [datanode_hostnames]
    :datanodes,         # Map of datanode_hostname -> %{pid: pid, blocks: [block_id], last_heartbeat: datetime}
    :replication_factor
  ]

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # FIXED: Accept both hostname AND the actual DataNode PID
  def register_datanode(hostname, datanode_pid) do
    GenServer.call(__MODULE__, {:register_datanode, hostname, datanode_pid})
  end

  # FIXED: Accept hostname instead of PID (DataNode sends hostname)
  def heartbeat(hostname, blocks) do
    GenServer.cast(__MODULE__, {:heartbeat, hostname, blocks})
  end

  def store_file(filename, size, num_blocks) do
    GenServer.call(__MODULE__, {:store_file, filename, size, num_blocks}, 30_000)
  end

  def read_file(filename) do
    GenServer.call(__MODULE__, {:read_file, filename})
  end

  def delete_file(filename) do
    GenServer.call(__MODULE__, {:delete_file, filename})
  end

  def list_files do
    GenServer.call(__MODULE__, :list_files)
  end

  def get_block_locations(block_id) do
    GenServer.call(__MODULE__, {:get_block_locations, block_id})
  end

  def get_datanodes do
    GenServer.call(__MODULE__, :get_datanodes)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("NameNode starting...")
    state = %__MODULE__{
      file_registry: %{},
      block_locations: %{},
      datanodes: %{},
      replication_factor: 2
    }
    {:ok, state}
  end

  # FIXED: Use the explicitly provided DataNode PID for monitoring
  @impl true
  def handle_call({:register_datanode, hostname, datanode_pid}, _from, state) do
    Logger.info("DataNode registered: #{hostname} (#{inspect(datanode_pid)})")

    # Monitor the ACTUAL DataNode process (provided explicitly)
    Process.monitor(datanode_pid)

    new_datanodes = Map.put(state.datanodes, hostname, %{
      pid: datanode_pid,  # This is the actual DataNode PID from the other node
      hostname: hostname,
      blocks: [],
      last_heartbeat: DateTime.utc_now()
    })

    {:reply, :ok, %{state | datanodes: new_datanodes}}
  end

  # TODO: Change to accept pathfile for storing file
  @impl true
  def handle_call({:store_file, filename, size, num_blocks}, _from, state) do
    # Check if file already exists
    if Map.has_key?(state.file_registry, filename) do
      {:reply, {:error, :file_exists}, state}
    else
      block_ids = Enum.map(1..num_blocks, fn i ->
        "#{filename}_block_#{i}_#{:erlang.unique_integer([:positive])}"
      end)

      # Assign blocks to datanodes
      datanode_hostnames = Map.keys(state.datanodes)

      if Enum.empty?(datanode_hostnames) do
        {:reply, {:error, :no_datanodes}, state}
      else
        block_assignments = assign_blocks_to_datanodes(
          block_ids,
          datanode_hostnames,
          state.replication_factor
        )

        new_file_registry = Map.put(state.file_registry, filename, %{
          blocks: block_ids,
          size: size,
          created_at: DateTime.utc_now()
        })

        new_block_locations = Enum.reduce(block_assignments, state.block_locations,
          fn {block_id, nodes}, acc ->
            Map.put(acc, block_id, nodes)
          end
        )

        new_state = %{state |
          file_registry: new_file_registry,
          block_locations: new_block_locations
        }

        {:reply, {:ok, block_assignments}, new_state}
      end
    end
  end

  @impl true
  def handle_call({:read_file, filename}, _from, state) do
    case Map.get(state.file_registry, filename) do
      nil ->
        {:reply, {:error, :file_not_found}, state}
      file_info ->
        # Get block locations for each block
        blocks_with_locations = Enum.map(file_info.blocks, fn block_id ->
          locations = Map.get(state.block_locations, block_id, [])
          {block_id, locations}
        end)

        result = %{
          filename: filename,
          size: file_info.size,
          blocks: blocks_with_locations,
          created_at: file_info.created_at
        }

        {:reply, {:ok, result}, state}
    end
  end

  @impl true
  def handle_call({:delete_file, filename}, _from, state) do
    case Map.get(state.file_registry, filename) do
      nil ->
        {:reply, {:error, :file_not_found}, state}
      file_info ->
        # Remove file from registry
        new_file_registry = Map.delete(state.file_registry, filename)

        # Remove block locations
        new_block_locations = Enum.reduce(file_info.blocks, state.block_locations,
          fn block_id, acc ->
            # Notify datanodes to delete blocks
            case Map.get(state.block_locations, block_id) do
              nil -> acc
              datanode_hostnames ->
                Enum.each(datanode_hostnames, fn hostname ->
                  case Map.get(state.datanodes, hostname) do
                    nil -> :ok
                    datanode_info ->
                      send(datanode_info.pid, {:delete_block, block_id})
                  end
                end)
                Map.delete(acc, block_id)
            end
          end
        )

        new_state = %{state |
          file_registry: new_file_registry,
          block_locations: new_block_locations
        }

        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call(:list_files, _from, state) do
    files = Enum.map(state.file_registry, fn {filename, info} ->
      %{
        filename: filename,
        size: info.size,
        num_blocks: length(info.blocks),

        created_at: info.created_at || DateTime.utc_now()
      }
    end)
    {:reply, files, state}
  end

  @impl true
  def handle_call({:get_block_locations, block_id}, _from, state) do
    locations = Map.get(state.block_locations, block_id, [])
    {:reply, locations, state}
  end

  @impl true
  def handle_call(:get_datanodes, _from, state) do
    datanodes = Enum.map(state.datanodes, fn {hostname, info} ->
      %{
        hostname: hostname,
        pid: info.pid,
        num_blocks: length(info.blocks),
        last_heartbeat: info.last_heartbeat
      }
    end)
    {:reply, datanodes, state}
  end

  # TODO: Change monitoring logic
  @impl true
  def handle_cast({:heartbeat, hostname, blocks}, state) do
    case Map.get(state.datanodes, hostname) do
      nil ->
        Logger.warning("Heartbeat from unknown DataNode: #{hostname}")
        {:noreply, state}
      datanode_info ->
        updated_datanode = %{datanode_info |
          blocks: blocks,
          last_heartbeat: DateTime.utc_now()
        }
        new_datanodes = Map.put(state.datanodes, hostname, updated_datanode)
        {:noreply, %{state | datanodes: new_datanodes}}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.warning("DataNode process down: #{inspect(pid)} - #{inspect(reason)}")

    # Find the DataNode by PID and remove it
    {hostname_to_remove, _} = Enum.find(state.datanodes, fn {_hostname, info} ->
      info.pid == pid
    end) || {nil, nil}

    case hostname_to_remove do
      nil ->
        {:noreply, state}
      hostname ->
        datanode_info = state.datanodes[hostname]
        Logger.warning("DataNode #{hostname} disconnected. Lost #{length(datanode_info.blocks)} blocks")

        # Remove from datanodes
        new_datanodes = Map.delete(state.datanodes, hostname)

        # TODO: Trigger block re-replication for lost blocks


        {:noreply, %{state | datanodes: new_datanodes}}
    end
  end


  # TODO: Change to priority list for mapping block to node assignment
  defp assign_blocks_to_datanodes(block_ids, datanode_hostnames, replication_factor) do
    num_datanodes = length(datanode_hostnames)
    actual_replication = min(replication_factor, num_datanodes)

    block_ids
    |> Enum.with_index()
    |> Enum.map(fn {block_id, idx} ->
      # Round-robin assignment with replication
      assigned_nodes =
        datanode_hostnames
        |> Stream.cycle()
        |> Stream.drop(idx)
        |> Enum.take(actual_replication)

      {block_id, assigned_nodes}
    end)
    |> Enum.into(%{})
  end
end
