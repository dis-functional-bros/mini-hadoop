defmodule MiniHadoop.Master.FileOperation do
  use GenServer
  require Logger

  alias MiniHadoop.Common.{Block, FileTask}
  alias MiniHadoop.Master.NameNode

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def submit_store_file(filename, file_path) do
    GenServer.call(__MODULE__, {:submit_store, filename, file_path})
  end

  def submit_read_file(filename) do
    GenServer.call(__MODULE__, {:submit_read, filename})
  end

  def read_block(block_id) do
    GenServer.call(__MODULE__, {:read_block, block_id})
  end

  def submit_delete_file(filename) do
    GenServer.call(__MODULE__, {:submit_delete, filename})
  end

  def get_operation_status(operation_id) do
    GenServer.call(__MODULE__, {:get_status, operation_id})
  end

  def list_operations do
    GenServer.call(__MODULE__, :list_operations)
  end

  def cancel_operation(operation_id) do
    GenServer.call(__MODULE__, {:cancel_operation, operation_id})
  end

  def get_operation_result(operation_id) do
    GenServer.call(__MODULE__, {:get_result, operation_id})
  end

  def wait_for_result(operation_id, timeout \\ 30_000) do
    case poll_for_completion(operation_id, timeout, System.monotonic_time(:millisecond)) do
      {:ok, %{result: result}} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def check_result(operation_id) do
    GenServer.call(__MODULE__, {:check_result, operation_id})
  end

  def store_file(filename, file_path) do
    case submit_store_file(filename, file_path) do
      {:ok, operation_id} -> wait_for_completion(operation_id)
      error -> error
    end
  end

  def read_file(filename) do
    case submit_read_file(filename) do
      {:ok, operation_id} -> wait_for_completion(operation_id)
      error -> error
    end
  end

  def delete_file(filename) do
    case submit_delete_file(filename) do
      {:ok, operation_id} -> wait_for_completion(operation_id)
      error -> error
    end
  end

  defp wait_for_completion(operation_id, timeout \\ 30_000) do
    case poll_until_complete(operation_id, timeout) do
      {:ok, %{result: result}} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp poll_until_complete(
         operation_id,
         timeout,
         start_time \\ System.monotonic_time(:millisecond)
       ) do
    case get_operation_status(operation_id) do
      {:ok, %{status: :completed} = op} ->
        {:ok, op}

      {:ok, %{status: :failed} = op} ->
        {:error, op.error_reason}

      {:ok, _op} ->
        if System.monotonic_time(:millisecond) - start_time > timeout do
          {:error, :timeout}
        else
          :timer.sleep(100)
          poll_until_complete(operation_id, timeout, start_time)
        end

      error ->
        error
    end
  end

  def init(_opts) do
    {:ok, %{operations: %{}, next_id: 1}}
  end

  def handle_call({:submit_store, filename, file_path}, _from, state) do
    operation_id = "store_#{state.next_id}"

    task =
      FileTask.new(%{id: operation_id, type: :store, filename: filename, file_path: file_path})

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, task),
        next_id: state.next_id + 1
    }

    Task.start(fn -> execute_store_operation(operation_id, task) end)
    {:reply, {:ok, operation_id}, new_state}
  end

  def handle_call({:submit_read, filename}, _from, state) do
    operation_id = "read_#{state.next_id}"
    task = FileTask.new(%{id: operation_id, type: :read, filename: filename})

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, task),
        next_id: state.next_id + 1
    }

    Task.start(fn -> execute_read_operation(operation_id, task) end)
    {:reply, {:ok, operation_id}, new_state}
  end

  def handle_call({:read_block, block_id}, _from, state) do
    # Step 1: Get block information from NameNode
    case NameNode.get_block_info(block_id) do
      {:ok, %{datanodes: datanodes}} ->
        # Step 2: Try to read from one of the datanodes
        case read_block_from_datanodes(block_id, datanodes) do
          {:ok, data} ->
            {:reply, {:ok, data}, state}

          {:error, reason} ->
            {:reply, {:error, "Failed to read block #{block_id}: #{reason}"}, state}
        end

      {:error, reason} ->
        {:reply, {:error, "NameNode error for block #{block_id}: #{reason}"}, state}
    end
  end

  def handle_call({:submit_delete, filename}, _from, state) do
    operation_id = "delete_#{state.next_id}"
    task = FileTask.new(%{id: operation_id, type: :delete, filename: filename})

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, task),
        next_id: state.next_id + 1
    }

    Task.start(fn -> execute_delete_operation(operation_id, task) end)
    {:reply, {:ok, operation_id}, new_state}
  end

  def handle_call({:get_status, operation_id}, _from, state) do
    case Map.get(state.operations, operation_id) do
      nil -> {:reply, {:error, :operation_not_found}, state}
      task -> {:reply, {:ok, task}, state}
    end
  end

  def handle_call(:list_operations, _from, state) do
    {:reply, {:ok, Map.values(state.operations)}, state}
  end

  def handle_call({:cancel_operation, operation_id}, _from, state) do
    case Map.get(state.operations, operation_id) do
      nil ->
        {:reply, {:error, :operation_not_found}, state}

      task when task.status in [:pending, :running] ->
        updated_task = FileTask.mark_failed(task, :cancelled, "Cancelled")
        new_operations = Map.put(state.operations, operation_id, updated_task)
        {:reply, :ok, %{state | operations: new_operations}}

      _ ->
        {:reply, {:error, :operation_not_cancellable}, state}
    end
  end

  def handle_call({:get_result, operation_id}, _from, state) do
    case Map.get(state.operations, operation_id) do
      nil ->
        {:reply, {:error, :operation_not_found}, state}

      %{status: :completed, result: result} ->
        {:reply, {:ok, result}, state}

      %{status: :failed, error_reason: reason} ->
        {:reply, {:error, reason}, state}

      %{status: status} when status in [:pending, :running] ->
        {:reply, {:error, :operation_not_completed}, state}
    end
  end

  def handle_call({:check_result, operation_id}, _from, state) do
    case Map.get(state.operations, operation_id) do
      nil ->
        {:reply, {:error, :operation_not_found}, state}

      %{status: :completed, result: result} ->
        {:reply, {:ok, result}, state}

      %{status: :failed, error_reason: reason} ->
        {:reply, {:error, :operation_failed, reason}, state}

      %{status: status} when status in [:pending, :running] ->
        {:reply, {:error, :operation_still_running}, state}
    end
  end

  def handle_cast({:update_task, operation_id, updated_task}, state) do
    Logger.info(
      "Task #{operation_id}: #{updated_task.message} (#{updated_task.progress}%) - #{updated_task.blocks_processed}/#{updated_task.total_blocks} blocks"
    )

    new_operations = Map.put(state.operations, operation_id, updated_task)
    {:noreply, %{state | operations: new_operations}}
  end

  defp execute_store_operation(operation_id, task) do
    task = FileTask.mark_running(task, "Calculating file size")
    update_task(operation_id, task)

    file_size = File.stat!(task.file_path).size
    num_blocks = Block.calculate_num_blocks(file_size)

    task = FileTask.update_progress(task, 0, num_blocks, "Allocating blocks")
    update_task(operation_id, task)

    case NameNode.store_file(task.filename, file_size, num_blocks) do
      {:ok, block_assignments} ->
        task = FileTask.update_progress(task, 0, num_blocks, "Storing blocks")
        update_task(operation_id, task)

        case stream_and_store_blocks(block_assignments, task.file_path, operation_id, task) do
          {:ok, final_task} ->
            completed_task =
              FileTask.mark_completed(final_task, "File stored successfully", "Completed")

            update_task(operation_id, completed_task)

          {:error, reason, final_task} ->
            failed_task = FileTask.mark_failed(final_task, reason, "Storage failed")
            update_task(operation_id, failed_task)
        end

      {:error, reason} ->
        failed_task = FileTask.mark_failed(task, reason, "Allocation failed")
        update_task(operation_id, failed_task)
    end
  end

  defp execute_read_operation(operation_id, task) do
    task = FileTask.mark_running(task, "Getting file info")
    update_task(operation_id, task)

    case NameNode.file_info(task.filename) do
      {:ok, file_info} ->
        task = FileTask.update_progress(task, 0, file_info.num_blocks, "Reading blocks")
        update_task(operation_id, task)

        case read_all_blocks(file_info.blocks, operation_id, task) do
          {:ok, content, final_task} ->
            completed_task = FileTask.mark_completed(final_task, {:ok, content}, "Completed")
            update_task(operation_id, completed_task)

          {:error, reason, final_task} ->
            failed_task = FileTask.mark_failed(final_task, reason, "Read failed")
            update_task(operation_id, failed_task)
        end

      {:error, reason} ->
        failed_task = FileTask.mark_failed(task, reason, "File not found")
        update_task(operation_id, failed_task)
    end
  end

  defp execute_delete_operation(operation_id, task) do
    task = FileTask.mark_running(task, "Getting file info")
    update_task(operation_id, task)

    case NameNode.file_info(task.filename) do
      {:ok, file_info} ->
        task = FileTask.update_progress(task, 0, file_info.num_blocks, "Deleting blocks")
        update_task(operation_id, task)

        case delete_all_blocks(file_info.blocks, operation_id, task) do
          :ok ->
            NameNode.delete_file(task.filename)
            completed_task = FileTask.mark_completed(task, :ok, "Completed")
            update_task(operation_id, completed_task)

          {:error, reason} ->
            failed_task = FileTask.mark_failed(task, reason, "Deletion failed")
            update_task(operation_id, failed_task)
        end

      {:error, :file_not_found} ->
        completed_task = FileTask.mark_completed(task, :ok, "File already deleted")
        update_task(operation_id, completed_task)

      {:error, reason} ->
        failed_task = FileTask.mark_failed(task, reason, "File info failed")
        update_task(operation_id, failed_task)
    end
  end

  defp stream_and_store_blocks(block_assignments, file_path, operation_id, task) do
    total_blocks = map_size(block_assignments)
    block_ids = Map.keys(block_assignments) |> Enum.sort()
    block_size = Block.get_block_size()

    stream =
      Stream.resource(
        fn ->
          case File.open(file_path, [:read, :binary, :raw]) do
            {:ok, file} -> {:ok, {file, 0}}
            {:error, reason} -> {:error, reason}
          end
        end,
        fn
          {:ok, {file, index}} ->
            case :file.read(file, block_size) do
              {:ok, chunk} ->
                block_id = Enum.at(block_ids, index)
                datanodes = Map.get(block_assignments, block_id, [])

                updated_task =
                  FileTask.update_progress(
                    task,
                    index + 1,
                    total_blocks,
                    "Storing block #{index + 1}"
                  )

                update_task(operation_id, updated_task)

                case store_block_on_datanodes(block_id, chunk, datanodes) do
                  :ok ->
                    {[chunk], {:ok, {file, index + 1}}}

                  {:error, reason} ->
                    {[], {:error, reason}}
                end

              :eof ->
                {:halt, {:ok, {file, index}}}

              {:error, reason} ->
                {[], {:error, reason}}
            end

          {:error, reason} ->
            {[], {:error, reason}}
        end,
        fn
          {:ok, {file, _index}} -> File.close(file)
          _ -> :ok
        end
      )

    # Process the stream and handle results
    result =
      stream
      |> Enum.reduce({:ok, task}, fn
        _chunk, {:ok, current_task} -> {:ok, current_task}
        _chunk, {:error, reason} -> {:error, reason}
      end)

    case result do
      {:ok, final_task} ->
        final_task =
          FileTask.update_progress(final_task, total_blocks, total_blocks, "All blocks stored")

        {:ok, final_task}

      {:error, reason} ->
        {:error, reason, task}
    end
  end

  defp read_all_blocks(blocks, operation_id, task) do
    total_blocks = length(blocks)

    {block_results, final_task} =
      Enum.reduce(blocks, {[], task}, fn {block_id, datanodes}, {results, current_task} ->
        updated_task =
          FileTask.update_progress(
            current_task,
            length(results) + 1,
            total_blocks,
            "Reading block #{length(results) + 1}"
          )

        update_task(operation_id, updated_task)

        result = read_block_from_datanodes(block_id, datanodes)
        {[result | results], updated_task}
      end)

    block_results = Enum.reverse(block_results)

    case Enum.any?(block_results, &match?({:error, _}, &1)) do
      false ->
        content = Enum.map(block_results, fn {:ok, data} -> data end) |> Enum.join("")
        {:ok, content, final_task}

      true ->
        {:error, :read_failed, final_task}
    end
  end

  defp delete_all_blocks(blocks, operation_id, task) do
    total_blocks = length(blocks)

    results =
      Enum.with_index(blocks)
      |> Enum.map(fn {{block_id, datanodes}, index} ->
        updated_task =
          FileTask.update_progress(task, index + 1, total_blocks, "Deleting block #{index + 1}")

        update_task(operation_id, updated_task)
        delete_block_from_datanodes(block_id, datanodes)
      end)

    if Enum.all?(results, &(&1 == :ok)), do: :ok, else: {:error, :deletion_failed}
  end

  defp store_block_on_datanodes(block_id, chunk, datanodes) do
    results =
      Enum.map(datanodes, fn datanode_pid ->
        store_block_on_datanode(datanode_pid, block_id, chunk)
      end)

    if Enum.any?(results, &(&1 == :ok)), do: :ok, else: {:error, :storage_failed}
  end

  defp store_block_on_datanode(datanode_pid, block_id, block_data) do
    case :rpc.call(
           node(datanode_pid),
           MiniHadoop.Slave.DataNode,
           :store_block,
           [block_id, block_data],
           30_000
         ) do
      :ok -> :ok
      _ -> {:error, :storage_failed}
    end
  end

  defp read_block_from_datanodes(block_id, datanodes) do
    Enum.reduce_while(datanodes, {:error, :not_found}, fn datanode_pid, _acc ->
      case read_block_from_datanode(datanode_pid, block_id) do
        {:ok, data} -> {:halt, {:ok, data}}
        _ -> {:cont, {:error, :not_found}}
      end
    end)
  end

  defp read_block_from_datanode(datanode_pid, block_id) do
    case :rpc.call(node(datanode_pid), MiniHadoop.Slave.DataNode, :read_block, [block_id], 15_000) do
      {:ok, data} -> {:ok, data}
      _ -> {:error, :read_failed}
    end
  end

  defp delete_block_from_datanodes(block_id, datanodes) do
    results =
      Enum.map(datanodes, fn datanode_pid ->
        delete_block_from_datanode(datanode_pid, block_id)
      end)

    if Enum.any?(results, &(&1 == :ok)), do: :ok, else: {:error, :deletion_failed}
  end

  defp delete_block_from_datanode(datanode_pid, block_id) do
    case :rpc.call(
           node(datanode_pid),
           MiniHadoop.Slave.DataNode,
           :delete_block,
           [block_id],
           10_000
         ) do
      :ok -> :ok
      {:error, :block_not_found} -> :ok
      _ -> {:error, :deletion_failed}
    end
  end

  defp update_task(operation_id, task) do
    GenServer.cast(__MODULE__, {:update_task, operation_id, task})
  end

  defp poll_for_completion(operation_id, timeout, start_time) do
    case get_operation_status(operation_id) do
      {:ok, %{status: :completed} = op} ->
        {:ok, op}

      {:ok, %{status: :failed} = op} ->
        {:error, op.error_reason}

      {:ok, _op} ->
        if System.monotonic_time(:millisecond) - start_time > timeout do
          {:error, :timeout}
        else
          :timer.sleep(100)
          poll_for_completion(operation_id, timeout, start_time)
        end

      error ->
        error
    end
  end
end
