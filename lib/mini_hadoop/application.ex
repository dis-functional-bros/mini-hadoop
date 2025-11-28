defmodule MiniHadoop.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    # Save application start time
    Application.put_env(:mini_hadoop, :start_time, DateTime.utc_now())

    # Log environment information
    Logger.info("Starting MiniHadoop in #{Mix.env()} environment")
    Logger.info("Node: #{Node.self()}")

    # Get configuration with environment-aware defaults
    node_type = Application.get_env(:mini_hadoop, :node_type, "worker")
    master_node = Application.get_env(:mini_hadoop, :master_node, "master@master") |> String.to_atom()
    node_name = Application.get_env(:mini_hadoop, :node_name, "unnamed_node")

    # Log configuration being used
    Logger.info("Configuration: node_type=#{node_type}, node_name=#{node_name}, master_node=#{master_node}")

    children =
      case String.downcase(node_type) do
        "master" ->
          Logger.info("Starting MASTER Node: #{node_name}")
          master_children()

        "worker" ->
          Logger.info("Starting WORKER Node: #{node_name}")
          worker_children(master_node)

        invalid_type ->
          Logger.error("Invalid NODE_TYPE: #{invalid_type}. Must be 'master' or 'worker'")
          raise "Invalid NODE_TYPE"
      end

    opts = [strategy: :one_for_one, name: MiniHadoop.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp master_children do
    [
      {DynamicSupervisor, strategy: :one_for_one, name: MiniHadoop.Job.JobSupervisor},
      MiniHadoop.Master.MasterNode,
      MiniHadoop.Master.FileOperation,
      MiniHadoop.Master.ComputeOperation,
    ]
  end

  defp worker_children(master_node) do
    # Get worker-specific configuration
    max_concurrent_tasks = Application.get_env(:mini_hadoop, :max_concurrent_tasks_on_runner, 4)
    heartbeat_interval = Application.get_env(:mini_hadoop, :heartbeat_interval_ms, 50_000)

    Logger.info("Worker config: max_concurrent_tasks=#{max_concurrent_tasks}, heartbeat_interval=#{heartbeat_interval}ms")

    [
      {Task.Supervisor, name: MiniHadoop.ComputeTask.TaskSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: MiniHadoop.ComputeTask.RunnerAndStorageSupervisor},
      {MiniHadoop.Worker.WorkerNode, [
        master: master_node,
        heartbeat_interval: heartbeat_interval
      ]},
    ]
  end

  # Optional: Add environment-specific startup logic
  defp environment_specific_setup do
    case Mix.env() do
      :dev ->
        Logger.info("Development mode")

      :test ->
        Logger.info("Test mode")

      :prod ->
        Logger.info("Production mode")
    end
  end
end
