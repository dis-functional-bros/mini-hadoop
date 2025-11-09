defmodule MiniHadoop.Client do
  @moduledoc """
  MiniHadoop Client - API for interacting with the Distributed File System.
  Now uses FileOperation for all file operations.
  """

  alias MiniHadoop.Master.FileOperation
  alias MiniHadoop.Master.NameNode

  @doc """
  Store a file in the distributed file system.
  """
  def store_file(filename, file_path, splitter_function \\ &MiniHadoop.Common.Block.default_splitter/2) do
    FileOperation.store_file(filename, file_path, splitter_function)
  end

  @spec read_file(binary()) :: any()
  @doc """
  Read a file from the distributed file system.
  """
  def read_file(filename) when is_binary(filename) do
    FileOperation.read_file(filename)
  end

  @doc """
  Delete a file from the distributed file system.
  """
  def delete_file(filename) when is_binary(filename) do
    FileOperation.delete_file(filename)
  end

  @doc """
  List all files in the distributed file system.
  """
  def list_files do
    NameNode.list_files()
  end

  def read_block(block_id) do
    FileOperation.read_block(block_id)
  end

  @doc """
  Get information about the cluster (for debugging/monitoring).
  """
  def cluster_info do
    alias MiniHadoop.Master.NameNode

    datanodes = NameNode.get_datanodes()
    files = list_files()

    %{
      datanodes: datanodes,
      num_files: length(files),
      total_blocks: count_total_blocks(files),
      files: files
    }
  end

  @doc """
  Submit an asynchronous file operation and get operation ID for tracking.
  """
  def submit_store_file(filename, file_path) do
    FileOperation.submit_store_file(filename, file_path)
  end

  def submit_read_file(filename) do
    FileOperation.submit_read_file(filename)
  end

  def submit_delete_file(filename) do
    FileOperation.submit_delete_file(filename)
  end


  @doc """
  Get operation status by ID.
  """
  def get_operation_status(operation_id) do
    FileOperation.get_operation_status(operation_id)
  end

  def get_operation_result(operation_id) do
    FileOperation.get_operation_result(operation_id)
  end

  @doc """
  List all operations with their current status.
  """
  def list_operations do
    FileOperation.list_operations()
  end

  defp count_total_blocks(files) do
    Enum.reduce(files, 0, fn file, acc -> acc + file.num_blocks end)
  end
end
