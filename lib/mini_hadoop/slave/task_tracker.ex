defmodule MiniHadoop.Slave.TaskTracker do
  @moduledoc """
  TaskTracker executes map and reduce tasks assigned by JobTracker.
  
  NOTE: This is a skeleton implementation. MapReduce functionality not yet implemented.
  """
  use GenServer
  require Logger

  defstruct [
    :hostname,
    :master_node,
    :status,
    :current_task
  ]

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def execute_task(task) do
    GenServer.cast(__MODULE__, {:execute_task, task})
  end

  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    hostname = opts[:hostname] || to_string(Node.self())
    master_node = opts[:master_node] || :master@master
    
    Logger.info("TaskTracker starting on #{hostname}... (skeleton mode)")
    
    state = %__MODULE__{
      hostname: hostname,
      master_node: master_node,
      status: :idle,
      current_task: nil
    }
    
    # TODO: Register with JobTracker when implemented
    # if Node.connect(master_node) do
    #   :rpc.call(master_node, MiniHadoop.Master.JobTracker, :register_task_tracker, [hostname])
    # end
    
    {:ok, state}
  end

  @impl true
  def handle_cast({:execute_task, _task}, state) do
    Logger.warning("TaskTracker: execute_task not yet implemented")
    {:noreply, state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    {:reply, state.status, state}
  end

  @impl true
  def handle_info({:execute_task, _task}, state) do
    Logger.warning("TaskTracker: MapReduce execution not yet implemented")
    {:noreply, state}
  end
end
