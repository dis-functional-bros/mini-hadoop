defmodule MiniHadoop.Slave.DataNode do
  @moduledoc """
  DataNode stores actual data blocks and reports to NameNode.
  Handles physical storage and retrieval of blocks.
  """
  use GenServer
  require Logger

  defstruct [
    :hostname,
    :master_node,
    # Map of block_id -> binary_data
    :blocks,
    :storage_path,
    :registration_attempts,
    :is_registered
  ]

  # Constants
  @initial_registration_delay 2_000
  @registration_retry_delay 5_000
  @heartbeat_interval 5_000
  @rpc_timeout 10_000

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def store_block(block_id, data) do
    GenServer.call(__MODULE__, {:store_block, block_id, data}, 30_000)
  end

  def read_block(block_id) do
    GenServer.call(__MODULE__, {:read_block, block_id})
  end

  def delete_block(block_id) do
    GenServer.call(__MODULE__, {:delete_block, block_id})
  end

  def list_blocks do
    GenServer.call(__MODULE__, :list_blocks)
  end

  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    hostname = opts[:hostname] || to_string(Node.self())
    master_node = opts[:master_node] || :master@master
    storage_path = opts[:storage_path] || "/tmp/mini_hadoop_data/#{hostname}"

    Logger.info("DataNode starting on #{hostname}...")

    state = %__MODULE__{
      hostname: hostname,
      master_node: master_node,
      blocks: %{},
      storage_path: storage_path,
      registration_attempts: 0,
      is_registered: false
    }

    # Ensure storage directory exists
    File.mkdir_p!(state.storage_path)

    # Load existing blocks from disk
    state = load_blocks_from_disk(state)

    # Start registration process asynchronously
    Process.send_after(self(), :attempt_registration, @initial_registration_delay)

    {:ok, state}
  end

  @impl true
  def handle_call({:store_block, block_id, data}, _from, state) do
    Logger.info("Storing block: #{block_id} (#{byte_size(data)} bytes)")

    # Store in memory
    new_blocks = Map.put(state.blocks, block_id, data)

    # Store on disk
    block_path = Path.join(state.storage_path, block_id)

    case File.write(block_path, data) do
      :ok ->
        {:reply, :ok, %{state | blocks: new_blocks}}

      {:error, reason} ->
        Logger.error("Failed to write block #{block_id}: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:read_block, block_id}, _from, state) do
    case Map.get(state.blocks, block_id) do
      nil ->
        # Try to read from disk
        block_path = Path.join(state.storage_path, block_id)

        case File.read(block_path) do
          {:ok, data} ->
            # Cache in memory
            new_blocks = Map.put(state.blocks, block_id, data)
            {:reply, {:ok, data}, %{state | blocks: new_blocks}}

          {:error, _} ->
            {:reply, {:error, :block_not_found}, state}
        end

      data ->
        {:reply, {:ok, data}, state}
    end
  end

  @impl true
  def handle_call({:delete_block, block_id}, _from, state) do
    Logger.info("Deleting block: #{block_id}")

    # Remove from memory
    new_blocks = Map.delete(state.blocks, block_id)

    # Remove from disk
    block_path = Path.join(state.storage_path, block_id)
    File.rm(block_path)

    {:reply, :ok, %{state | blocks: new_blocks}}
  end

  @impl true
  def handle_call(:list_blocks, _from, state) do
    block_ids = Map.keys(state.blocks)
    {:reply, block_ids, state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      hostname: state.hostname,
      master_node: state.master_node,
      is_registered: state.is_registered,
      registration_attempts: state.registration_attempts,
      block_count: map_size(state.blocks),
      storage_path: state.storage_path
    }

    {:reply, status, state}
  end

  @impl true
  def handle_info(:attempt_registration, state) do
    new_attempts = state.registration_attempts + 1
    state = %{state | registration_attempts: new_attempts}

    case register_with_namenode(state) do
      {:ok, new_state} ->
        Logger.info("✅ Successfully registered with NameNode after #{new_attempts} attempts")
        Process.send_after(self(), :send_heartbeat, 1000)
        {:noreply, new_state}

      {:error, reason} ->
        Logger.warning(
          "🔄 Registration attempt #{new_attempts} failed: #{reason}. Retrying in #{@registration_retry_delay}ms..."
        )

        Process.send_after(self(), :attempt_registration, @registration_retry_delay)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:send_heartbeat, state) do
    if state.is_registered do
      case send_heartbeat(state) do
        :ok ->
          Logger.debug("Heartbeat acknowledged by NameNode")

        {:error, reason} ->
          Logger.warning("Heartbeat failed: #{reason}. Will retry and may need to re-register")
          # If heartbeat fails consistently, we might need to re-register
          if state.registration_attempts > 10 do
            Process.send_after(self(), :attempt_registration, @registration_retry_delay)
          end
      end
    end

    Process.send_after(self(), :send_heartbeat, @heartbeat_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info({:delete_block, block_id}, state) do
    # Handle delete request from NameNode
    Logger.info("Received delete request for block: #{block_id}")

    new_blocks = Map.delete(state.blocks, block_id)
    block_path = Path.join(state.storage_path, block_id)
    File.rm(block_path)

    {:noreply, %{state | blocks: new_blocks}}
  end

  @impl true
  def handle_info({:store_block, block_id, data}, state) do
    # Handle store request from NameNode (async)
    Logger.info("Received store request for block: #{block_id}")

    new_blocks = Map.put(state.blocks, block_id, data)
    block_path = Path.join(state.storage_path, block_id)

    case File.write(block_path, data) do
      :ok ->
        {:noreply, %{state | blocks: new_blocks}}

      {:error, reason} ->
        Logger.error("Failed to store block #{block_id}: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  ## Private Functions

  defp register_with_namenode(state) do
    try do
      # First, ensure we can connect to the master node
      case Node.ping(state.master_node) do
        :pong ->
          Logger.debug(
            "Master node #{state.master_node} is reachable, attempting registration..."
          )

          # Attempt registration with timeout
          case :rpc.call(
                 state.master_node,
                 MiniHadoop.Master.NameNode,
                 :register_datanode,
                 # Pass self() as the actual DataNode PID
                 [state.hostname, self()],
                 @rpc_timeout
               ) do
            :ok ->
              Logger.info("✅ Registration successful with NameNode at #{state.master_node}")
              {:ok, %{state | is_registered: true}}

            {:error, reason} ->
              Logger.error("❌ NameNode registration rejected: #{reason}")
              {:error, reason}

            :badrpc ->
              Logger.warning("🔄 RPC call to NameNode failed - master might not be ready")
              {:error, :rpc_failed}

            other ->
              Logger.error("❌ Unexpected response from NameNode: #{inspect(other)}")
              {:error, :unexpected_response}
          end

        :pang ->
          Logger.warning("🌐 Master node #{state.master_node} is not reachable")
          {:error, :master_unreachable}
      end
    rescue
      error ->
        Logger.error("💥 Exception during NameNode registration: #{inspect(error)}")
        {:error, :exception}
    end
  end

  defp send_heartbeat(state) do
    try do
      block_ids = Map.keys(state.blocks)

      case :rpc.call(
             state.master_node,
             MiniHadoop.Master.NameNode,
             :heartbeat,
             [state.hostname, block_ids],
             @rpc_timeout
           ) do
        :ok ->
          :ok

        {:error, reason} ->
          {:error, reason}

        :badrpc ->
          {:error, :rpc_failed}

        other ->
          Logger.error("Unexpected heartbeat response: #{inspect(other)}")
          {:error, :unexpected_response}
      end
    rescue
      error ->
        Logger.error("Heartbeat exception: #{inspect(error)}")
        {:error, :exception}
    end
  end

  defp load_blocks_from_disk(state) do
    case File.ls(state.storage_path) do
      {:ok, files} ->
        blocks =
          Enum.reduce(files, %{}, fn filename, acc ->
            block_path = Path.join(state.storage_path, filename)

            case File.read(block_path) do
              {:ok, data} -> Map.put(acc, filename, data)
              {:error, _} -> acc
            end
          end)

        if map_size(blocks) > 0 do
          Logger.info("Loaded #{map_size(blocks)} blocks from disk")
        end

        %{state | blocks: blocks}

      {:error, _} ->
        state
    end
  end
end
