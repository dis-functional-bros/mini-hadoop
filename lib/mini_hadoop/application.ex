defmodule MiniHadoop.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    # Save application start time
    Application.put_env(:mini_hadoop, :start_time, DateTime.utc_now())

    # Get node configuration from environment
    node_type = System.get_env("NODE_TYPE", "slave")
    master_node = System.get_env("MASTER_NODE", "master@master") |> String.to_atom()

    Logger.info("Node #{Node.self()} started with cookie: #{Node.get_cookie()}")

    # Determine which children to start based on node type
    children = case String.downcase(node_type) do
      "master" ->
        Logger.info("Starting MASTER node (NameNode only)")
        master_children()

      "slave" ->
        Logger.info("Starting SLAVE node (DataNode only)")
        slave_children(master_node)

      _ ->
        Logger.error("Invalid NODE_TYPE: #{node_type}. Must be 'master' or 'slave'")
        raise "Invalid NODE_TYPE"
    end

    opts = [strategy: :one_for_one, name: MiniHadoop.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp master_children do
    [
      MiniHadoop.Master.NameNode,
      MiniHadoop.Master.FileOperation
    ]
  end

  defp slave_children(master_node) do
    node_name = Node.self() |> to_string()

    [
      {MiniHadoop.Slave.DataNode, [hostname: node_name, master_node: master_node]}
    ]
  end
end
