defmodule MiniHadoop.Master.NameNodeTest do
  use ExUnit.Case, async: false
  alias MiniHadoop.Master.NameNode

  defmodule FakeDataNode do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def init(opts) do
      {:ok,
       %{
         test_pid: opts[:test_pid],
         response: opts[:response] || :ok
       }}
    end

    def handle_call({:send_block_to_datanode, block_id, target_pid}, _from, state) do
      send(state.test_pid, {:send_block, self(), block_id, target_pid})
      {:reply, state.response, state}
    end
  end

  setup do
    {:ok, pid} = start_supervised(NameNode)
    %{name_node: pid}
  end

  defp register_test_datanode(hostname, pid \\ nil) do
    pid =
      case pid do
        nil ->
          {:ok, spawned} = Task.start(fn -> Process.sleep(:infinity) end)
          spawned

        custom ->
          custom
      end

    on_exit(fn -> Process.exit(pid, :kill) end)
    :ok = NameNode.register_datanode(hostname, pid)
    pid
  end

  describe "datanode registration" do
    test "can register a datanode" do
      {:ok, pid} = Task.start(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pid, :kill) end)
      assert :ok = NameNode.register_datanode("test_datanode_1", pid)
    end

    test "can register multiple datanodes" do
      {:ok, pid1} = Task.start(fn -> Process.sleep(:infinity) end)
      {:ok, pid2} = Task.start(fn -> Process.sleep(:infinity) end)
      {:ok, pid3} = Task.start(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Enum.each([pid1, pid2, pid3], &Process.exit(&1, :kill))
      end)

      assert :ok = NameNode.register_datanode("test_datanode_1", pid1)
      assert :ok = NameNode.register_datanode("test_datanode_2", pid2)
      assert :ok = NameNode.register_datanode("test_datanode_3", pid3)
    end
  end

  describe "file operations" do
    setup do
      register_test_datanode("datanode_1")
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
      register_test_datanode("datanode_1")

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
      register_test_datanode("datanode_1")
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
      register_test_datanode("datanode_1")
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
      register_test_datanode("datanode_1")
      register_test_datanode("datanode_2")

      {:ok, assignments} = NameNode.store_file("test.txt", 1000, 3)

      Enum.each(assignments, fn {_block_id, nodes} ->
        assert length(nodes) == 2
      end)
    end

    test "replication limited by number of available datanodes" do
      register_test_datanode("datanode_1")

      {:ok, assignments} = NameNode.store_file("test.txt", 1000, 2)

      Enum.each(assignments, fn {_block_id, nodes} ->
        assert length(nodes) == 1
      end)
    end
  end

  describe "re-replication" do
    test "re-replicates blocks when a datanode is removed", %{name_node: _pid} do
      {:ok, source_pid} = FakeDataNode.start_link(test_pid: self())
      failing_pid = register_test_datanode("datanode_b")
      target_pid = register_test_datanode("datanode_c")
      register_test_datanode("datanode_a", source_pid)

      {:ok, assignments} = NameNode.store_file("important.txt", 100, 1)
      block_id = assignments |> Map.keys() |> List.first()

      NameNode.heartbeat("datanode_a", [block_id])
      NameNode.heartbeat("datanode_b", [block_id])
      NameNode.heartbeat("datanode_c", [])

      Process.exit(failing_pid, :kill)

      assert_receive {:send_block, ^source_pid, ^block_id, ^target_pid}, 500

      locations = NameNode.get_block_locations(block_id)
      assert Enum.sort(locations) == Enum.sort(["datanode_a", "datanode_c"])

      datanodes = NameNode.get_datanodes()

      assert %{} =
               Enum.find(datanodes, fn dn ->
                 dn.hostname == "datanode_c" and dn.num_blocks == 1
               end)
    end
  end
end
