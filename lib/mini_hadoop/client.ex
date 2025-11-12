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
  def store_file(filename, file_path) do
    FileOperation.store_file(filename, file_path)
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

  def file_info(filename) do
    NameNode.file_info(filename)
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

    # Get application start time and calculate uptime
    app_start_time = Application.get_env(:mini_hadoop, :start_time, DateTime.utc_now())
    now = DateTime.utc_now()
    uptime_seconds = DateTime.diff(now, app_start_time, :second)

    days = div(uptime_seconds, 86400)
    hours = div(rem(uptime_seconds, 86400), 3600)
    minutes = div(rem(uptime_seconds, 3600), 60)
    seconds = rem(uptime_seconds, 60)

    uptime = %{
      days: days,
      hours: hours,
      minutes: minutes,
      seconds: seconds
    }

    %{
      datanodes: datanodes,
      num_files: length(files),
      total_blocks: count_total_blocks(files),
      files: files,
      uptime: uptime,
      started_at: app_start_time
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
