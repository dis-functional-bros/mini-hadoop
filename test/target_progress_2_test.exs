defmodule MiniHadoop.TargetProgress2Test do
  use ExUnit.Case, async: false

  alias MiniHadoop.Master.NameNode
  alias MiniHadoop.Slave.DataNode

  describe "Target Progress 2 – Master/Slave handshake" do
    test "master node registers an incoming slave node" do
      {:ok, _pid} = start_supervised(NameNode)
      datanode_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(datanode_pid, :kill) end)

      assert :ok = NameNode.register_datanode("test_slave@local", datanode_pid)

      registered_hosts =
        NameNode.get_datanodes()
        |> Enum.map(& &1.hostname)

      assert "test_slave@local" in registered_hosts
    end

    test "slave node keeps the provided master node address" do
      storage_path = Path.join(System.tmp_dir!(), "mini_hadoop_target_progress_2")
      File.rm_rf!(storage_path)

      {:ok, pid} =
        start_supervised(
          {DataNode,
           [
             hostname: "target_progress_slave",
             master_node: :master_progress@local,
             storage_path: storage_path
           ]}
        )

      on_exit(fn ->
        File.rm_rf(storage_path)
      end)

      state = :sys.get_state(pid)
      assert state.master_node == :master_progress@local
      assert state.hostname == "target_progress_slave"
    end

    test "master stores datanode metadata after registration" do
      {:ok, _pid} = start_supervised(NameNode)
      datanode_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(datanode_pid, :kill) end)

      assert :ok = NameNode.register_datanode("handshake_slave@local", datanode_pid)

      datanode_info =
        NameNode.get_datanodes()
        |> Enum.find(fn dn -> dn.hostname == "handshake_slave@local" end)

      refute is_nil(datanode_info)
      assert datanode_info.pid == datanode_pid
      assert datanode_info.num_blocks == 0
      assert %DateTime{} = datanode_info.last_heartbeat
    end

    test "slave node tracks failed registration attempts" do
      storage_path = Path.join(System.tmp_dir!(), "mini_hadoop_target_progress_2_retry")
      File.rm_rf!(storage_path)

      {:ok, pid} =
        start_supervised(
          {DataNode,
           [
             hostname: "retry_slave",
             master_node: :unreachable_master@local,
             storage_path: storage_path
           ]}
        )

      on_exit(fn ->
        File.rm_rf(storage_path)
      end)

      Process.sleep(2_200)

      state = :sys.get_state(pid)
      refute state.is_registered
      assert state.registration_attempts >= 1
      assert state.master_node == :unreachable_master@local
    end

    test "heartbeat updates datanode metadata" do
      {:ok, _pid} = start_supervised(NameNode)
      datanode_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(datanode_pid, :kill) end)

      assert :ok = NameNode.register_datanode("heartbeat_slave@local", datanode_pid)

      initial =
        NameNode.get_datanodes()
        |> Enum.find(&(&1.hostname == "heartbeat_slave@local"))

      refute is_nil(initial)
      initial_timestamp = initial.last_heartbeat

      :ok = NameNode.heartbeat("heartbeat_slave@local", ["block-1", "block-2"])
      Process.sleep(50)

      updated =
        NameNode.get_datanodes()
        |> Enum.find(&(&1.hostname == "heartbeat_slave@local"))

      refute is_nil(updated)
      assert updated.num_blocks == 2
      assert DateTime.compare(updated.last_heartbeat, initial_timestamp) in [:gt, :eq]
    end

    test "heartbeat from unknown slave leaves registry unchanged" do
      {:ok, _pid} = start_supervised(NameNode)

      :ok = NameNode.heartbeat("ghost_slave@local", ["ghost-block"])
      Process.sleep(20)

      assert [] = NameNode.get_datanodes()
    end

    test "master removes datanode when monitored process terminates" do
      {:ok, _pid} = start_supervised(NameNode)
      datanode_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(datanode_pid, :kill) end)

      assert :ok = NameNode.register_datanode("down_slave@local", datanode_pid)

      assert NameNode.get_datanodes()
             |> Enum.any?(&(&1.hostname == "down_slave@local"))

      Process.exit(datanode_pid, :kill)
      Process.sleep(50)

      refute NameNode.get_datanodes()
             |> Enum.any?(&(&1.hostname == "down_slave@local"))
    end

    test "re-registering the same hostname updates the tracked PID" do
      {:ok, _pid} = start_supervised(NameNode)
      first_pid = spawn(fn -> Process.sleep(:infinity) end)
      second_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn ->
        Process.exit(first_pid, :kill)
        Process.exit(second_pid, :kill)
      end)

      assert :ok = NameNode.register_datanode("duplicate_slave@local", first_pid)
      assert :ok = NameNode.register_datanode("duplicate_slave@local", second_pid)

      datanode =
        NameNode.get_datanodes()
        |> Enum.find(&(&1.hostname == "duplicate_slave@local"))

      refute is_nil(datanode)
      assert datanode.pid == second_pid
    end
  end
end
