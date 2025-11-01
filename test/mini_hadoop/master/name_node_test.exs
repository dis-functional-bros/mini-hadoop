defmodule MiniHadoop.Master.NameNodeTest do
  use ExUnit.Case, async: false
  alias MiniHadoop.Master.NameNode

  setup do
    {:ok, pid} = start_supervised(NameNode)
    %{name_node: pid}
  end

  describe "datanode registration" do
    test "can register a datanode" do
      assert :ok = NameNode.register_datanode("test_datanode_1")
    end

    test "can register multiple datanodes" do
      assert :ok = NameNode.register_datanode("test_datanode_1")
      assert :ok = NameNode.register_datanode("test_datanode_2")
      assert :ok = NameNode.register_datanode("test_datanode_3")
    end
  end

  describe "file operations" do
    setup do
      NameNode.register_datanode("datanode_1")
      :ok
    end

    test "can store a file" do
      assert {:ok, assignments} = NameNode.store_file("test.txt", 1000, 2)
      assert map_size(assignments) == 2
    end

    test "cannot store file with same name twice" do
      {:ok, _} = NameNode.store_file("test.txt", 1000, 1)
      assert {:error, :file_exists} = NameNode.store_file("test.txt", 500, 1)
    end

    test "returns error when no datanodes available" do
      {:ok, _pid} = start_supervised({NameNode, []}, id: :temp_namenode)
      assert {:error, :no_datanodes} = 
        GenServer.call(:temp_namenode, {:store_file, "test.txt", 1000, 2})
    end

    test "can read file metadata" do
      NameNode.store_file("test.txt", 1000, 2)
      
      assert {:ok, info} = NameNode.read_file("test.txt")
      assert info.filename == "test.txt"
      assert info.size == 1000
      assert length(info.blocks) == 2
    end

    test "read returns error for non-existent file" do
      assert {:error, :file_not_found} = NameNode.read_file("nonexistent.txt")
    end

    test "can delete a file" do
      NameNode.store_file("test.txt", 1000, 1)
      assert :ok = NameNode.delete_file("test.txt")
      assert {:error, :file_not_found} = NameNode.read_file("test.txt")
    end

    test "delete returns error for non-existent file" do
      assert {:error, :file_not_found} = NameNode.delete_file("nonexistent.txt")
    end
  end

  describe "list files" do
    test "returns empty list when no files" do
      assert [] = NameNode.list_files()
    end

    test "lists all stored files" do
      NameNode.register_datanode("datanode_1")
      
      NameNode.store_file("file1.txt", 1000, 1)
      NameNode.store_file("file2.txt", 2000, 2)
      NameNode.store_file("file3.txt", 3000, 3)
      
      files = NameNode.list_files()
      assert length(files) == 3
      
      filenames = Enum.map(files, & &1.filename)
      assert "file1.txt" in filenames
      assert "file2.txt" in filenames
      assert "file3.txt" in filenames
    end

    test "list includes file metadata" do
      NameNode.register_datanode("datanode_1")
      NameNode.store_file("test.txt", 1500, 2)
      
      [file] = NameNode.list_files()
      assert file.filename == "test.txt"
      assert file.size == 1500
      assert file.num_blocks == 2
      assert %DateTime{} = file.created_at
    end
  end

  describe "block locations" do
    test "can retrieve block locations" do
      NameNode.register_datanode("datanode_1")
      {:ok, assignments} = NameNode.store_file("test.txt", 1000, 1)
      
      block_id = assignments |> Map.keys() |> List.first()
      locations = NameNode.get_block_locations(block_id)
      
      assert is_list(locations)
      assert length(locations) > 0
    end

    test "returns empty list for non-existent block" do
      locations = NameNode.get_block_locations("nonexistent_block")
      assert locations == []
    end
  end

  describe "replication" do
    test "assigns blocks to multiple datanodes for replication" do
      NameNode.register_datanode("datanode_1")
      NameNode.register_datanode("datanode_2")
      
      {:ok, assignments} = NameNode.store_file("test.txt", 1000, 3)
      
      # Each block should be assigned to multiple nodes (replication factor 2)
      Enum.each(assignments, fn {_block_id, nodes} ->
        assert length(nodes) == 2
      end)
    end

    test "replication limited by number of available datanodes" do
      NameNode.register_datanode("datanode_1")
      
      {:ok, assignments} = NameNode.store_file("test.txt", 1000, 2)
      
      # Only 1 datanode available, so replication is limited to 1
      Enum.each(assignments, fn {_block_id, nodes} ->
        assert length(nodes) == 1
      end)
    end
  end
end
