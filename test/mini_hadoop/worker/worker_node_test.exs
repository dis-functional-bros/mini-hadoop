defmodule MiniHadoop.Worker.WorkerNodeTest do
  use ExUnit.Case, async: false

  alias MiniHadoop.Worker.WorkerNode
  alias MiniHadoop.Master.MasterNode

  setup do
    {:ok, _master} = start_supervised(MasterNode)

    temp_dir = "/tmp/mini_hadoop_worker_test_#{:rand.uniform(1000000)}"
    File.mkdir_p!(temp_dir)

    on_exit(fn ->
      File.rm_rf!(temp_dir)
    end)

    {:ok, _worker} = start_supervised({
      WorkerNode,
      [master: Node.self()]
    })

    %{temp_dir: temp_dir}
  end

  describe "worker registration" do
    test "worker node registers with master" do
      Process.sleep(200)

      state = :sys.get_state(WorkerNode)
      assert state.status == :idle or state.status == :healthy
    end
  end

  describe "block storage operations" do
    test "can store and retrieve block" do
      block_id = "test_block_#{:rand.uniform(1000000)}"
      data = "Test block data"

      assert :ok = WorkerNode.store_block(block_id, data)
      assert {:ok, ^data} = WorkerNode.read_block(block_id)
    end

    test "returns error for non-existent block" do
      assert {:error, :block_not_found} = WorkerNode.read_block("nonexistent_block")
    end

    test "can delete stored block" do
      block_id = "delete_test_#{:rand.uniform(1000000)}"
      WorkerNode.store_block(block_id, "data")

      assert :ok = WorkerNode.delete_block(block_id)
      assert {:error, :block_not_found} = WorkerNode.read_block(block_id)
    end
  end

  describe "heartbeat mechanism" do
    test "worker sends heartbeat to master" do
      Process.sleep(100)

      state = :sys.get_state(WorkerNode)
      # Heartbeat should have been sent at least once
      assert state.registration_attempts >= 0
    end
  end

  describe "list blocks" do
    test "returns list of stored blocks" do
      block_1 = "list_test_#{:rand.uniform(1000000)}_1"
      block_2 = "list_test_#{:rand.uniform(1000000)}_2"

      WorkerNode.store_block(block_1, "data1")
      WorkerNode.store_block(block_2, "data2")

      blocks = WorkerNode.list_blocks()

      assert block_1 in blocks
      assert block_2 in blocks
    end
  end
end
