defmodule MiniHadoop do
  @moduledoc """
  MiniHadoop Client - API for interacting with the Distributed File System.

  Provides a simplified interface for file operations across the distributed cluster.

  ## Main API Functions:

  - `store_file/2` - Store a file in the DFS
  - `read_file/1` - Read a file from the DFS
  - `delete_file/1` - Delete a file from the DFS
  - `list_files/0` - List all files in the DFS
  - `cluster_info/0` - Get cluster status information
  """

  alias MiniHadoop.Common.Block
  alias MiniHadoop.Master.NameNode

  @default_timeout 30_000
  @max_read_retries 3

  @doc """
  Store a file in the distributed file system.

  ## Examples

      iex> MiniHadoop.Client.store_file("test.txt", "Hello World")
      {:ok, "File stored successfully: test.txt"}

      iex> MiniHadoop.Client.store_file("existing.txt", "data")
      {:error, :file_exists}
  """
  def store_file(filename, content) when is_binary(content) do
    with {:ok, block_assignments} <- request_file_allocation(filename, content),
         :ok <- store_blocks_on_datanodes(content, block_assignments) do
      {:ok, "File stored successfully: #{filename}"}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def store_file(_filename, _content) do
    {:error, :invalid_content}
  end

  @doc """
  Read a file from the distributed file system.

  ## Examples

      iex> MiniHadoop.Client.read_file("test.txt")
      {:ok, "Hello World"}

      iex> MiniHadoop.Client.read_file("nonexistent.txt")
      {:error, :file_not_found}
  """
  def read_file(filename) when is_binary(filename) do
    case NameNode.read_file(filename) do
      {:ok, file_info} ->
        assemble_file_from_blocks(file_info)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete a file from the distributed file system.

  ## Examples

      iex> MiniHadoop.Client.delete_file("test.txt")
      :ok

      iex> MiniHadoop.Client.delete_file("nonexistent.txt")
      {:error, :file_not_found}
  """
  def delete_file(filename) when is_binary(filename) do
    NameNode.delete_file(filename)
  end

  @doc """
  List all files in the distributed file system.

  ## Examples

      MiniHadoop.Client.list_files()
      #=> [
      #     %{filename: "test.txt", size: 1024, num_blocks: 1, created_at: ~U[2024-01-01 12:00:00Z]},
      #     %{filename: "data.csv", size: 2048, num_blocks: 1, created_at: ~U[2024-01-01 12:05:00Z]}
      #   ]
  """
  def list_files do
    NameNode.list_files()
  end

  @doc """
  Get information about the cluster (for debugging/monitoring).

  ## Examples

      iex> MiniHadoop.Client.cluster_info()
      #=> %{
      #     datanodes: ["datanode1@cluster", "datanode2@cluster"],
      #     num_files: 5,
      #     files: [
      #       %{filename: "test.txt", size: 1024, num_blocks: 1, created_at: ~U[2024-01-01 12:00:00Z]}
      #     ]
      #   }
  """
  def cluster_info do
    datanodes = NameNode.get_datanodes()
    files = list_files()

    %{
      datanodes: datanodes,
      num_files: length(files),
      total_blocks: count_total_blocks(files),
      files: files
    }
  end

  ## Private Helper Functions

  defp request_file_allocation(filename, content) do
    size = byte_size(content)
    blocks = Block.split_into_blocks(content)
    num_blocks = length(blocks)

    NameNode.store_file(filename, size, num_blocks)
  end

  defp store_blocks_on_datanodes(content, block_assignments) do
    blocks = Block.split_into_blocks(content)

    results = Enum.zip(blocks, Map.keys(block_assignments))
    |> Enum.map(fn {block, block_id} ->
      datanodes = Map.get(block_assignments, block_id, [])
      replicate_block(block_id, block.data, datanodes)
    end)

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, :block_storage_failed}
    end
  end

  defp replicate_block(block_id, block_data, datanodes) do
    replication_results = Enum.map(datanodes, fn datanode_pid ->
      store_block_on_datanode(datanode_pid, block_id, block_data)
    end)

    if Enum.any?(replication_results, &(&1 == :ok)) do
      :ok  # At least one replica succeeded
    else
      {:error, :replication_failed}
    end
  end

  defp store_block_on_datanode(datanode_pid, block_id, block_data) do
    case :rpc.call(
      node(datanode_pid),
      MiniHadoop.Slave.DataNode,
      :store_block,
      [block_id, block_data],
      @default_timeout
    ) do
      :ok -> :ok
      {:error, _reason} -> {:error, :storage_failed}
      :badrpc -> {:error, :communication_failed}
    end
  end

  defp assemble_file_from_blocks(file_info) do
    block_results = Enum.map(file_info.blocks, fn {block_id, datanode_pids} ->
      read_block_with_retry(block_id, datanode_pids, @max_read_retries)
    end)

    case check_block_read_results(block_results) do
      {:ok, block_data} ->
        content = Enum.join(block_data, "")
        {:ok, content}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_block_with_retry(_block_id, _datanodes, 0), do: {:error, :max_retries_exceeded}

  defp read_block_with_retry(block_id, datanodes, retries_remaining) do
    case read_block_from_nodes(block_id, datanodes) do
      {:ok, data} -> {:ok, data}
      {:error, _} ->
        :timer.sleep(100)  # Brief delay before retry
        read_block_with_retry(block_id, datanodes, retries_remaining - 1)
    end
  end

  defp read_block_from_nodes(_block_id, []), do: {:error, :no_datanodes}

  defp read_block_from_nodes(block_id, [datanode_pid | rest]) do
    case :rpc.call(
      node(datanode_pid),
      MiniHadoop.Slave.DataNode,
      :read_block,
      [block_id],
      @default_timeout
    ) do
      {:ok, data} -> {:ok, data}
      {:error, _} -> read_block_from_nodes(block_id, rest)
      :badrpc -> read_block_from_nodes(block_id, rest)
    end
  end

  defp check_block_read_results(block_results) do
    if Enum.all?(block_results, &match?({:ok, _}, &1)) do
      block_data = Enum.map(block_results, fn {:ok, data} -> data end)
      {:ok, block_data}
    else
      failed_blocks = Enum.count(block_results, &match?({:error, _}, &1))
      {:error, {:block_read_failed, failed_blocks}}
    end
  end

  defp count_total_blocks(files) do
    Enum.reduce(files, 0, fn file, acc -> acc + file.num_blocks end)
  end
end
