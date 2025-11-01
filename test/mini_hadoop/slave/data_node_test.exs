defmodule MiniHadoop.Slave.DataNodeTest do
  use ExUnit.Case, async: false
  alias MiniHadoop.Slave.DataNode

  setup do
    # Create temp directory for testing
    temp_dir = "/tmp/mini_hadoop_test_#{:rand.uniform(1000000)}"
    File.mkdir_p!(temp_dir)
    
    # Start DataNode with test configuration
    {:ok, pid} = start_supervised({DataNode, [
      hostname: "test_datanode",
      master_node: Node.self(),
      storage_path: temp_dir
    ]})
    
    on_exit(fn ->
      File.rm_rf!(temp_dir)
    end)
    
    %{data_node: pid, temp_dir: temp_dir}
  end

  describe "block storage" do
    test "can store a block" do
      block_id = "test_block_1"
      data = "Hello, MiniHadoop DFS!"
      
      assert :ok = DataNode.store_block(block_id, data)
    end

    test "can store multiple blocks" do
      assert :ok = DataNode.store_block("block_1", "data1")
      assert :ok = DataNode.store_block("block_2", "data2")
      assert :ok = DataNode.store_block("block_3", "data3")
    end

    test "stores block on disk", %{temp_dir: temp_dir} do
      block_id = "disk_test_block"
      data = "Test data for disk persistence"
      
      DataNode.store_block(block_id, data)
      
      block_path = Path.join(temp_dir, block_id)
      assert File.exists?(block_path)
      assert {:ok, ^data} = File.read(block_path)
    end

    test "can store large blocks" do
      block_id = "large_block"
      data = String.duplicate("x", 1024 * 1024)  # 1MB
      
      assert :ok = DataNode.store_block(block_id, data)
      assert {:ok, ^data} = DataNode.read_block(block_id)
    end
  end

  describe "block retrieval" do
    test "can read a stored block" do
      block_id = "test_block"
      data = "Test data for reading"
      
      DataNode.store_block(block_id, data)
      
      assert {:ok, ^data} = DataNode.read_block(block_id)
    end

    test "returns error for non-existent block" do
      assert {:error, :block_not_found} = DataNode.read_block("nonexistent_block")
    end

    test "can read block from disk if not in memory", %{temp_dir: temp_dir} do
      block_id = "persistent_block"
      data = "Persistent data"
      
      # Write directly to disk
      block_path = Path.join(temp_dir, block_id)
      File.write!(block_path, data)
      
      # Should be able to read it
      assert {:ok, ^data} = DataNode.read_block(block_id)
    end
  end

  describe "block deletion" do
    test "can delete a block" do
      block_id = "delete_test"
      data = "Data to be deleted"
      
      DataNode.store_block(block_id, data)
      assert :ok = DataNode.delete_block(block_id)
      assert {:error, :block_not_found} = DataNode.read_block(block_id)
    end

    test "deletes block from disk", %{temp_dir: temp_dir} do
      block_id = "disk_delete_test"
      data = "Data on disk"
      
      DataNode.store_block(block_id, data)
      block_path = Path.join(temp_dir, block_id)
      assert File.exists?(block_path)
      
      DataNode.delete_block(block_id)
      refute File.exists?(block_path)
    end
  end

  describe "block listing" do
    test "can list all blocks" do
      DataNode.store_block("block_1", "data1")
      DataNode.store_block("block_2", "data2")
      DataNode.store_block("block_3", "data3")
      
      blocks = DataNode.list_blocks()
      
      assert "block_1" in blocks
      assert "block_2" in blocks
      assert "block_3" in blocks
      assert length(blocks) == 3
    end

    test "returns empty list when no blocks stored" do
      blocks = DataNode.list_blocks()
      assert blocks == []
    end
  end

  describe "persistence" do
    test "loads blocks from disk on startup", %{temp_dir: temp_dir} do
      # Write some blocks to disk
      File.write!(Path.join(temp_dir, "block_1"), "data1")
      File.write!(Path.join(temp_dir, "block_2"), "data2")
      
      # Start a new DataNode with same storage path
      {:ok, _pid} = start_supervised({DataNode, [
        hostname: "persistent_test",
        master_node: Node.self(),
        storage_path: temp_dir
      ]}, id: :persistent_datanode)
      
      # Give it time to load
      Process.sleep(100)
      
      # Should be able to read the blocks
      assert {:ok, "data1"} = GenServer.call(:persistent_datanode, {:read_block, "block_1"})
      assert {:ok, "data2"} = GenServer.call(:persistent_datanode, {:read_block, "block_2"})
    end
  end
end
