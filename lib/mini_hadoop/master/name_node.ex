defmodule MiniHadoop.Master.NameNode do
  @moduledoc """
  NameNode manages file system metadata and block locations.
  Handles file operations: store, read, delete, list
  """
  use GenServer
  require Logger

  @transfer_timeout 30_000

  defstruct [
    # Map of filename -> %{blocks: [block_id], size: int, created_at: datetime}
    :file_registry,
    # Map of block_id -> [datanode_hostnames]
    :block_locations,
    # Map of datanode_hostname -> %{pid: pid, blocks: [block_id], last_heartbeat: datetime}
    :datanodes,
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

  def file_info(filename) do
    GenServer.call(__MODULE__, {:file_info, filename})
  end

  #
  def block_info(block_id) do
    GenServer.call(__MODULE__, {:block_info, block_id})
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

    new_datanodes =
      Map.put(state.datanodes, hostname, %{
        # This is the actual DataNode PID from the other node
        pid: datanode_pid,
        hostname: hostname,
        blocks: [],
        last_heartbeat: DateTime.utc_now()
      })

    {:reply, :ok, %{state | datanodes: new_datanodes}}
  end

  @impl true
  def handle_call({:store_file, filename, size, num_blocks}, _from, state) do
    if Map.has_key?(state.file_registry, filename) do
      {:reply, {:error, :file_exists}, state}
    else
      block_ids =
        Enum.map(1..num_blocks, fn i ->
          "#{filename}_block_#{i}_#{:erlang.unique_integer([:positive])}"
        end)

      # Get datanode info (hostname -> %{pid: pid, ...})
      datanodes_info = state.datanodes

      if Enum.empty?(datanodes_info) do
        {:reply, {:error, :no_datanodes}, state}
      else
        # Pass datanodes_info to get PIDs
        block_assignments =
          assign_blocks_to_datanodes(
            block_ids,
            datanodes_info,
            state.replication_factor
          )

        new_file_registry =
          Map.put(state.file_registry, filename, %{
            blocks: block_ids,
            size: size,
            created_at: DateTime.utc_now()
          })

        # Store hostnames in block_locations for metadata
        new_block_locations =
          Enum.reduce(block_assignments, state.block_locations, fn
            {block_id, datanode_pids}, acc ->
              # Convert PIDs back to hostnames for metadata storage
              hostnames =
                Enum.map(datanode_pids, fn pid ->
                  find_hostname_by_pid(pid, state.datanodes)
                end)

              Map.put(acc, block_id, hostnames)
          end)

        new_state = %{
          state
          | file_registry: new_file_registry,
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
        blocks_with_locations =
          Enum.map(file_info.blocks, fn block_id ->
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

        # Remove block locations from metadata (blocks are already deleted by FileOperation)
        new_block_locations =
          Enum.reduce(file_info.blocks, state.block_locations, fn block_id, acc ->
            Map.delete(acc, block_id)
          end)

        updated_datanodes =
          Enum.reduce(file_info.blocks, state.datanodes, fn block_id, acc ->
            hostnames = Map.get(state.block_locations, block_id, [])

            Enum.reduce(hostnames, acc, fn hostname, acc2 ->
              if Map.has_key?(acc2, hostname) do
                Map.update!(acc2, hostname, fn info ->
                  current_blocks = info.blocks || []
                  %{info | blocks: List.delete(current_blocks, block_id)}
                end)
              else
                acc2
              end
            end)
          end)

        new_state = %{
          state
          | file_registry: new_file_registry,
            block_locations: new_block_locations,
            datanodes: updated_datanodes
        }

        Logger.info(
          "File metadata deleted: #{filename} (#{length(file_info.blocks)} blocks removed from metadata)"
        )

        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call(:list_files, _from, state) do
    files =
      Enum.map(state.file_registry, fn {filename, info} ->
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
    datanodes =
      for {hostname, info} <- state.datanodes do
        # Ambil semua blok yang disimpan di DataNode ini
        blocks_on_node =
          state.block_locations
          |> Enum.filter(fn {_block_id, hosts} -> hostname in hosts end)
          |> Enum.map(fn {block_id, _} -> block_id end)

        # Hitung total byte yang digunakan oleh blok-blok ini
        total_bytes =
          Enum.reduce(state.file_registry, 0, fn {_filename, file_info}, acc ->
            total_blocks = length(file_info.blocks)

            # Hindari pembagian 0
            avg_block_size =
              if total_blocks > 0, do: file_info.size / total_blocks, else: 0

            # Hitung berapa blok file ini ada di node ini
            blocks_here =
              Enum.count(file_info.blocks, fn block_id -> block_id in blocks_on_node end)

            acc + trunc(avg_block_size * blocks_here)
          end)

        %{
          hostname: hostname,
          pid: info.pid,
          num_blocks: length(info.blocks),
          used_storage: format_bytes(total_bytes),
          last_heartbeat: info.last_heartbeat
        }
      end

    {:reply, datanodes, state}
  end

  @impl true
  def handle_call({:file_info, filename}, _from, state) do
    case Map.get(state.file_registry, filename) do
      nil ->
        {:reply, {:error, :file_not_found}, state}

      file_info ->
        # Get block information with datanode PIDs
        blocks_with_datanodes =
          Enum.map(file_info.blocks, fn block_id ->
            hostnames = Map.get(state.block_locations, block_id, [])
            # Convert hostnames back to PIDs for file operations
            datanode_pids =
              Enum.map(hostnames, fn hostname ->
                case Map.get(state.datanodes, hostname) do
                  nil -> nil
                  info -> info.pid
                end
              end)
              |> Enum.filter(& &1)

            {block_id, datanode_pids}
          end)

        file_info_result = %{
          filename: filename,
          size: file_info.size,
          num_blocks: length(file_info.blocks),
          blocks: blocks_with_datanodes,
          created_at: file_info.created_at
        }

        {:reply, {:ok, file_info_result}, state}
    end
  end

  @impl true
  def handle_call({:block_info, block_id}, _from, state) do
    case Map.get(state.block_locations, block_id) do
      nil ->
        {:reply, {:error, :block_not_found}, state}

      hostnames ->
        # Convert hostnames to datanode PIDs
        datanode_pids =
          Enum.map(hostnames, fn hostname ->
            case Map.get(state.datanodes, hostname) do
              nil -> nil
              info -> info.pid
            end
          end)
          |> Enum.filter(& &1)

        if Enum.empty?(datanode_pids) do
          {:reply, {:error, :no_datanodes_available}, state}
        else
          block_info = %{
            block_id: block_id,
            datanodes: datanode_pids
          }

          {:reply, {:ok, block_info}, state}
        end
    end
  end

  # TODO: Change monitoring logic
  @impl true
  def handle_cast({:heartbeat, hostname, blocks}, state) do
    case Map.get(state.datanodes, hostname) do
      nil ->
        Logger.warning("Heartbeat from unknown DataNode: #{hostname}")
        {:noreply, state}

      datanode_info ->
        updated_datanode = %{datanode_info | blocks: blocks, last_heartbeat: DateTime.utc_now()}
        new_datanodes = Map.put(state.datanodes, hostname, updated_datanode)
        {:noreply, %{state | datanodes: new_datanodes}}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.warning("DataNode process down: #{inspect(pid)} - #{inspect(reason)}")

    # Find the DataNode by PID and remove it
    {hostname_to_remove, _} =
      Enum.find(state.datanodes, fn {_hostname, info} ->
        info.pid == pid
      end) || {nil, nil}

    case hostname_to_remove do
      nil ->
        {:noreply, state}

      hostname ->
        datanode_info = state.datanodes[hostname]

        lost_blocks = datanode_info.blocks || []

        Logger.warning(
          "DataNode #{hostname} disconnected. Lost #{length(lost_blocks)} blocks"
        )

        # Remove from datanodes
        datanodes_without_failed = Map.delete(state.datanodes, hostname)

        {updated_block_locations, updated_datanodes} =
          rebalance_blocks_after_failure(
            lost_blocks,
            hostname,
            state.block_locations,
            datanodes_without_failed,
            state.replication_factor
          )

        new_state = %{
          state
          | datanodes: updated_datanodes,
            block_locations: updated_block_locations
        }

        {:noreply, new_state}
    end
  end

  defp format_bytes(0), do: "0 B"
  defp format_bytes(bytes) when is_integer(bytes) and bytes > 0 do
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]

    # Hitung berapa kali bisa dibagi 1024
    power = floor(:math.log(bytes) / :math.log(1024))
    power = min(power, length(units) - 1)  # jangan sampai out of range

    value = bytes / :math.pow(1024, power)
    formatted = Float.round(value, 2)

    "#{formatted} #{Enum.at(units, power)}"
  end

  defp assign_blocks_to_datanodes(block_ids, datanodes_info, replication_factor) do
    # Initialize node entries with current load
    datanode_entries =
      for {hostname, info} <- datanodes_info do
        load =
          case info.blocks do
            blocks when is_list(blocks) -> length(blocks)
            _ -> 0
          end

        %{hostname: hostname, load: load}
      end

    actual_replication = min(length(datanode_entries), replication_factor)

    # Reduce over blocks to create assignments
    {assignments, _} =
      Enum.reduce(block_ids, {%{}, datanode_entries}, fn block_id, {acc, nodes} ->
        # Sort by load (ascending), then by hostname for consistency
        sorted_nodes = Enum.sort_by(nodes, &{&1.load, &1.hostname})

        # Take the least loaded nodes
        {selected, remaining} = Enum.split(sorted_nodes, actual_replication)

        # Update load for selected nodes
        updated_selected = Enum.map(selected, fn node -> %{node | load: node.load + 1} end)

        # Combine back all nodes
        updated_nodes = updated_selected ++ remaining

        # Get PIDs for selected nodes
        assigned_pids =
          selected
          |> Enum.map(& &1.hostname)
          |> Enum.map(&datanodes_info[&1].pid)

        # Add to assignments
        {Map.put(acc, block_id, assigned_pids), updated_nodes}
      end)

    assignments
  end

  defp find_hostname_by_pid(pid, datanodes) do
    Enum.find_value(datanodes, fn {hostname, info} ->
      if info.pid == pid, do: hostname
    end)
  end

  defp rebalance_blocks_after_failure(block_ids, failed_hostname, block_locations, datanodes, replication_factor) do
    Enum.reduce(block_ids, {block_locations, datanodes}, fn block_id, {block_locs_acc, datanodes_acc} ->
      {remaining_hosts, pruned_block_locs} =
        remove_block_host(block_locs_acc, block_id, failed_hostname)

      cond do
        remaining_hosts == [] ->
          Logger.error("All replicas lost for block #{block_id}. No sources available for re-replication.")
          {pruned_block_locs, datanodes_acc}

        length(remaining_hosts) >= replication_factor ->
          {pruned_block_locs, datanodes_acc}

        true ->
          needed = replication_factor - length(remaining_hosts)

          attempt_block_rereplication(
            block_id,
            remaining_hosts,
            needed,
            pruned_block_locs,
            datanodes_acc
          )
      end
    end)
  end

  defp remove_block_host(block_locations, block_id, hostname) do
    hosts = Map.get(block_locations, block_id, [])
    updated_hosts = List.delete(hosts, hostname)

    updated_block_locations =
      case updated_hosts do
        [] -> Map.delete(block_locations, block_id)
        _ -> Map.put(block_locations, block_id, updated_hosts)
      end

    {updated_hosts, updated_block_locations}
  end

  defp attempt_block_rereplication(_block_id, [], _needed, block_locations, datanodes) do
    {block_locations, datanodes}
  end

  defp attempt_block_rereplication(block_id, source_hosts, needed, block_locations, datanodes) do
    candidates =
      datanodes
      |> Enum.reject(fn {hostname, _} -> hostname in source_hosts end)
      |> Enum.reject(fn {hostname, _} ->
        hostname in Map.get(block_locations, block_id, [])
      end)
      |> Enum.sort_by(fn {hostname, info} ->
        load =
          case info.blocks do
            blocks when is_list(blocks) -> length(blocks)
            _ -> 0
          end

        {load, hostname}
      end)
      |> Enum.take(needed)

    if Enum.empty?(candidates) do
      Logger.warning(
        "Unable to re-replicate block #{block_id}: no candidate DataNodes available."
      )

      {block_locations, datanodes}
    else
      Enum.reduce(Enum.with_index(candidates), {block_locations, datanodes}, fn
        {{target_hostname, target_info}, idx}, {block_locs_acc, datanodes_acc} ->
          source_hostname = Enum.at(source_hosts, rem(idx, length(source_hosts)))

          case request_block_transfer(
                 block_id,
                 Map.get(datanodes_acc, source_hostname),
                 target_info
               ) do
            :ok ->
              Logger.info(
                "Re-replicated block #{block_id} from #{source_hostname} to #{target_hostname}"
              )

              updated_block_locs =
                Map.update(block_locs_acc, block_id, [target_hostname], fn hosts ->
                  hosts ++ [target_hostname]
                end)

              updated_datanodes =
                update_datanode_blocks(datanodes_acc, target_hostname, block_id)

              {updated_block_locs, updated_datanodes}

            {:error, reason} ->
              Logger.error(
                "Failed to re-replicate block #{block_id} from #{source_hostname} to #{target_hostname}: #{inspect(reason)}"
              )

              {block_locs_acc, datanodes_acc}
          end
      end)
    end
  end

  defp request_block_transfer(_block_id, nil, _target_info), do: {:error, :source_unavailable}

  defp request_block_transfer(block_id, source_info, target_info) do
    try do
      GenServer.call(
        source_info.pid,
        {:send_block_to_datanode, block_id, target_info.pid},
        @transfer_timeout
      )
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp update_datanode_blocks(datanodes, hostname, block_id) do
    case Map.fetch(datanodes, hostname) do
      {:ok, info} ->
        current_blocks =
          case info.blocks do
            blocks when is_list(blocks) -> blocks
            _ -> []
          end

        new_blocks =
          if block_id in current_blocks do
            current_blocks
          else
            current_blocks ++ [block_id]
          end

        Map.put(datanodes, hostname, %{info | blocks: new_blocks})

      :error ->
        datanodes
    end
  end
end
