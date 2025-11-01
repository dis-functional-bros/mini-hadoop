defmodule MiniHadoop.IntegrationTest do
  use ExUnit.Case
  
  @moduletag :integration

  setup_all do
    # Start the master node component
    {:ok, _name_node} = start_supervised(MiniHadoop.Master.NameNode)
    
    # Start slave node component
    {:ok, _data_node} = start_supervised({MiniHadoop.Slave.DataNode, [
      hostname: "test_slave",
      master_node: Node.self(),
      storage_path: "/tmp/mini_hadoop_integration_test"
    ]})
    
    # Wait for node to register
    Process.sleep(100)
    
    on_exit(fn ->
      File.rm_rf("/tmp/mini_hadoop_integration_test")
    end)
    
    :ok
  end

  test "end-to-end file store and read" do
    filename = "integration_test.txt"
    content = "This is an integration test for MiniHadoop DFS"
    
    # Store file
    assert {:ok, message} = MiniHadoop.store_file(filename, content)
    assert message =~ "File stored successfully"
    
    # Read file
    assert {:ok, read_content} = MiniHadoop.read_file(filename)
    assert read_content == content
  end

  test "end-to-end file delete" do
    filename = "delete_test.txt"
    content = "File to be deleted"
    
    # Store file
    {:ok, _} = MiniHadoop.store_file(filename, content)
    
    # Verify it exists
    assert {:ok, _} = MiniHadoop.read_file(filename)
    
    # Delete file
    assert :ok = MiniHadoop.delete_file(filename)
    
    # Verify it's gone
    assert {:error, :file_not_found} = MiniHadoop.read_file(filename)
  end

  test "list files shows stored files" do
    # Store multiple files
    MiniHadoop.store_file("file1.txt", "content1")
    MiniHadoop.store_file("file2.txt", "content2")
    MiniHadoop.store_file("file3.txt", "content3")
    
    files = MiniHadoop.list_files()
    filenames = Enum.map(files, & &1.filename)
    
    assert "file1.txt" in filenames
    assert "file2.txt" in filenames
    assert "file3.txt" in filenames
  end

  test "can store and read large files" do
    filename = "large_file.txt"
    content = String.duplicate("Large data block ", 100000)  # ~1.7MB
    
    {:ok, _} = MiniHadoop.store_file(filename, content)
    {:ok, read_content} = MiniHadoop.read_file(filename)
    
    assert read_content == content
  end

  test "cluster_info returns cluster status" do
    info = MiniHadoop.cluster_info()
    
    assert Map.has_key?(info, :datanodes)
    assert Map.has_key?(info, :num_files)
    assert Map.has_key?(info, :files)
    assert length(info.datanodes) >= 1
  end
end
