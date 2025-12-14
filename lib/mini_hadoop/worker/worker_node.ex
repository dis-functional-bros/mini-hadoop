defmodule MiniHadoop.Worker.WorkerNode do
  use GenServer
  require Logger

  alias MiniHadoop.ComputeTask.RunnerAndStorageSupervisor

  @register_timeout 5000
  @max_retries 5
  @heartbeat_interval Application.compile_env(:mini_hadoop, :heartbeat_interval_ms, 50_000)

  defstruct [
    :pid,
    :hostname,
    :status,
    :running_task,
    :path,
    :master,
    :last_heartbeat,
    :registration_attempts,
    :blocks_count,
    :blocks,
    :task_runner_pid
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(args) do
    default_path = Application.get_env(:mini_hadoop, :data_base_path)
    :ok = File.mkdir_p(default_path)

    state = %__MODULE__{
      pid: self(),
      hostname: Node.self(),
      status: :online,
      running_task: nil,
      master: args[:master],
      path: default_path,
      registration_attempts: 0,
      blocks_count: 0,
      blocks: %{}
    }

    Process.send_after(self(), :register_master, @register_timeout)
    {:ok, state}
  end

  # ========== handle_call functions (grouped together) ==========

  @impl true
  def handle_call({:start_runner_and_storage, job_id, job_pid}, _from, state) do
    {:ok, storage_pid} = RunnerAndStorageSupervisor.start_temp_storage(job_id)
    {:ok, runner_pid} = RunnerAndStorageSupervisor.start_task_runner(job_id, job_pid, storage_pid)
    {:reply, {:ok, {self(),storage_pid, runner_pid}}, state}
  end

  @impl true
  def handle_call({:store_block, block_id, block_data}, _from, state) do
    file_path = Path.join(state.path, block_id)
    File.write!(file_path, block_data)

    new_blocks = Map.put(state.blocks, block_id, true)
    new_blocks_count = state.blocks_count + 1
    new_state = %{state | blocks: new_blocks, blocks_count: new_blocks_count, running_task: nil}

    result = trim_send_state(new_state)
    result = Map.put(result, :changed_block, block_id)

    {:reply, {:store, result}, new_state}
  end

  @impl true
  def handle_call({:retrieve_block, block_id}, _from, state) do
    block_path = Path.join(state.path, block_id)

    result =
      if Map.has_key?(state.blocks, block_id) do
        case File.read(block_path) do
          {:ok, data} -> {:ok, data}
          {:error, reason} -> {:error, reason}
        end
      else
        {:error, :block_not_found}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:delete_block, block_id}, _from, state) do
    block_path = Path.join(state.path, block_id)

    new_blocks_count = state.blocks_count - 1
    new_blocks = Map.delete(state.blocks, block_id)
    new_state = %{state | blocks: new_blocks, blocks_count: new_blocks_count}

    if Map.has_key?(state.blocks, block_id) do
      spawn(fn -> File.rm!(block_path) end)
    end

    result = trim_send_state(new_state)
    result = Map.put(result, :changed_block, block_id)

    {:reply, {:delete, result}, new_state}
  end

  @impl true
  def handle_call({:replicate_block_from, block_id, source_pid}, _from, state) do
    # 1. Fetch data directly from source worker
    case GenServer.call(source_pid, {:retrieve_block, block_id}, 30_000) do
      {:ok, block_data} ->
        # 2. Store locally (reuse logic)
        file_path = Path.join(state.path, block_id)
        File.write!(file_path, block_data)

        new_blocks = Map.put(state.blocks, block_id, true)
        new_blocks_count = state.blocks_count + 1
        new_state = %{state | blocks: new_blocks, blocks_count: new_blocks_count, running_task: nil}

        result = trim_send_state(new_state)
        result = Map.put(result, :changed_block, block_id)

        {:reply, {:store, result}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      other ->
        {:reply, {:error, {:unexpected_response, other}}, state}
    end
  end

  @impl true
  def handle_call({:get_worker_state}, _from, state) do
    result = trim_send_state(state)
    {:reply, {:ok, result}, state}
  end

  # ========== handle_info functions (grouped together) ==========

  @impl true
  def handle_info(:register_master, %{registration_attempts: attempts} = state)
      when attempts >= @max_retries do
    Logger.error("Worker #{state.hostname} failed to register after #{@max_retries} attempts.")
    {:stop, :registration_failed, state}
  end

  @impl true
  def handle_info(:register_master, state) do
    try do
      case GenServer.call(
             {MiniHadoop.Master.MasterNode, state.master},
             {:register_worker, trim_send_state(state)},
             @register_timeout
           ) do
        :ok ->
          Logger.info("Worker #{state.hostname} registered to master")
          Process.send_after(self(), :send_heartbeat, @heartbeat_interval)
          {:noreply, %{state | status: :idle, registration_attempts: 0}}

        {:error, reason} ->
          Logger.warning("Registration failed: #{reason}. Retrying...")
          Process.send_after(self(), :register_master, @register_timeout)
          {:noreply, %{state | registration_attempts: state.registration_attempts + 1}}
      end
    catch
      :exit, _ ->
        Logger.warning("Master unavailable. Retrying...")
        Process.send_after(self(), :register_master, @register_timeout)
        {:noreply, %{state | registration_attempts: state.registration_attempts + 1}}
    end
  end

  @impl true
  def handle_info(:send_heartbeat, %{master: master, hostname: hostname} = state) do
    try do
      GenServer.cast({MiniHadoop.Master.MasterNode, master}, {:receive_heartbeat, hostname})
      Process.send_after(self(), :send_heartbeat, @heartbeat_interval)
      {:noreply, %{state | last_heartbeat: DateTime.utc_now()}}
    catch
      :exit, _ ->
        Logger.warning("Master unavailable for heartbeat. Attempting re-registration...")
        Process.send_after(self(), :register_master, @register_timeout)
        {:noreply, %{state | status: :offline, registration_attempts: 0}}
    end
  end

  # ========== Private functions ==========

  defp trim_send_state(state) do
    Map.take(state, [
      :pid,
      :hostname,
      :status,
      :running_task,
      :blocks_count,
      :last_heartbeat,
      :registration_attempts
    ])
  end
end
