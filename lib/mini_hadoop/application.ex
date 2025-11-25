defmodule MiniHadoop.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    # Save application start time
    Application.put_env(:mini_hadoop, :start_time, DateTime.utc_now())

    node_type = System.get_env("NODE_TYPE", "worker")
    master_node = System.get_env("MASTER_NODE", "master@master") |> String.to_atom()

    children =
      case String.downcase(node_type) do
        "master" ->
          Logger.info("Starting MASTER Node : #{Node.self()}")

          [
            {DynamicSupervisor, strategy: :one_for_one, name: MiniHadoop.Job.JobSupervisor},
            MiniHadoop.Master.MasterNode,
            MiniHadoop.Master.FileOperation,
            {MiniHadoop.Master.ComputeOperation, [max_concurrent_jobs: 1]},
          ]

        "worker" ->
          Logger.info("Starting WORKER Node : #{Node.self()}")

          [
            {Task.Supervisor, name: MiniHadoop.ComputeTask.TaskSupervisor},
            {MiniHadoop.Worker.WorkerNode, [master: master_node]},
            {MiniHadoop.ComputeTask.TaskRunner, [max_concurrent_compute_tasks: 4]},
          ]

        _ ->
          Logger.error("Invalid NODE_TYPE: #{node_type}. Must be 'master' or 'slave'")
          raise "Invalid NODE_TYPE"
      end

    opts = [strategy: :one_for_one, name: MiniHadoop.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
