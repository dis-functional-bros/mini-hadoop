defmodule MiniHadoop.FileOperationsTest do
  use ExUnit.Case, async: false

  alias MiniHadoop.Master.FileOperation

  @moduletag :integration

  describe "File Operations" do
    test "store_file with valid file path" do
      # Create a temporary test file
      test_content = "Hello, MiniHadoop World!"
      temp_file = Path.join(System.tmp_dir!(), "test_file.txt")
      File.write!(temp_file, test_content)

      on_exit(fn -> File.rm_rf(temp_file) end)

      result = FileOperation.store_file("test_file.txt", temp_file)
      assert {:ok, _task_id} = result
    end

    test "store_file with non-existent file" do
      result = FileOperation.store_file("nonexistent.txt", "/path/to/nonexistent/file.txt")
      assert {:error, _reason} = result
    end

    test "retrieve_file with existing file" do
      # First store a file, then retrieve it
      test_content = "Test retrieve content"
      temp_file = Path.join(System.tmp_dir!(), "retrieve_test.txt")
      File.write!(temp_file, test_content)

      on_exit(fn -> File.rm_rf(temp_file) end)

      {:ok, _store_task_id} = FileOperation.store_file("retrieve_test.txt", temp_file)

      # Wait for store operation to complete
      Process.sleep(2_000)

      result = FileOperation.retrieve_file("retrieve_test.txt")
      assert {:ok, _retrieve_task_id} = result
    end

    test "delete_file removes file from DFS" do
      test_content = "Test delete content"
      temp_file = Path.join(System.tmp_dir!(), "delete_test.txt")
      File.write!(temp_file, test_content)

      on_exit(fn -> File.rm_rf(temp_file) end)

      {:ok, _store_task_id} = FileOperation.store_file("delete_test.txt", temp_file)
      Process.sleep(1_000)

      result = FileOperation.delete_file("delete_test.txt")
      assert {:ok, _delete_task_id} = result
    end
  end
end
