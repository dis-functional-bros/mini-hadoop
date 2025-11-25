defmodule MiniHadoop.Worker.WorkerNode do
  use GenServer
  require Logger
  alias MiniHadoop.MapFunction

  @register_timeout 5000
  @max_retries 5
  @heartbeat_interval 500_000

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

  def init(args) do
    default_path = args[:path] || "/app/MiniHadoop/data/"
    :ok = File.mkdir_p(default_path)

    # Start TaskRunner - provide default if nil
     max_tasks = args[:max_concurrent_compute_tasks] || 4
     {:ok, task_runner_pid} = MiniHadoop.ComputeTask.TaskRunner.start_link([
       max_concurrent_compute_tasks: max_tasks,
       worker_node_pid: self()
     ])


    state = %__MODULE__{
      pid: self(),
      hostname: Node.self(),
      status: :online,
      running_task: nil,
      master: args[:master],
      path: default_path,
      registration_attempts: 0,
      blocks_count: 0,
      blocks: %{},
      task_runner_pid: task_runner_pid,
    }

    Process.send_after(self(), :register_master, @register_timeout)
    {:ok, state}
  end

  # Add the missing handle_cast
  def handle_cast({:execute_task, task}, state) do
    Logger.info("Worker routing task: #{task.task_id} to TaskRunner")
    GenServer.cast(state.task_runner_pid, {:execute_task, task})
    {:noreply, state}
  end

  def handle_info(:register_master, %{registration_attempts: attempts} = state)
      when attempts >= @max_retries do
    Logger.error("Worker #{state.hostname} failed to register after #{@max_retries} attempts.")
    {:stop, :registration_failed, state}
  end


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
          {:noreply, %{state | status: :idle}}

        {:error, reason} ->
          Logger.warning("Registration failed: #{reason}. Retrying...")
          Process.send_after(self(), :register_master, @register_timeout)
          {:noreply, state}
      end
    catch
      :exit, _ ->
        Logger.warning("Master master unavailable. Retrying...")
        Process.send_after(self(), :register_master, @register_timeout)
        {:noreply, state}
    end
  end

  def handle_info(:send_heartbeat, %{master: master, hostname: hostname} = state) do
    try do
      GenServer.cast({MiniHadoop.Master.MasterNode, master}, {:receive_heartbeat, hostname})
      Process.send_after(self(), :send_heartbeat, @heartbeat_interval)
    catch
      :exit, _ -> %{state | status: :offline, registration_attempts: 0}
    end

    {:noreply, state}
  end



  def handle_call({:store_block, block_id, block_data}, _from, state) do
    file_path = Path.join(state.path, block_id)
    File.write!(file_path, block_data)

    new_blocks = Map.put(state.blocks, block_id, true)
    new_blocks_count = state.blocks_count + 1
    new_state = %{state | blocks: new_blocks, blocks_count: new_blocks_count, running_task: nil}
    result = trim_send_state(new_state)
    result = Map.put(result,:changed_block, block_id )
    {:reply, {:store, result}, new_state}
  end


  def handle_call({:run_map, block_id, map_module, context}, _from, state) do
    block_path = Path.join(state.path, block_id)
    context = context || %{}

    result =
      with true <- File.exists?(block_path),
           {:ok, data} <- File.read(block_path),
           {:ok, map_result} <- Map.execute(map_module, data, context) do
        {:ok, %{block_id: block_id, result: map_result}}
      else
        false -> {:error, :block_not_found}
        {:error, reason} -> {:error, reason}
      end

    case result do
      {:ok, payload} ->
        {:reply, {:ok, payload}, %{state | running_task: nil}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

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

    {:reply,{:delete, result}, new_state}
  end

  defp trim_send_state(state) do
    Map.take(state, [:pid, :hostname, :status, :running_task, :blocks_count, :last_heartbeat, :registration_attempts])
  end
end
