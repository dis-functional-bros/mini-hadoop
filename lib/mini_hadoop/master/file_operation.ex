defmodule MiniHadoop.Master.FileOperation do
  use GenServer
  require Logger
  alias MiniHadoop.Common

  @default_timeout 3_000_000

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{operations: %{}, next_id: 1}, name: __MODULE__)
  end

  # ---------------------
  # Sync operations
  def store_file(filename, file_path) do
    GenServer.call(__MODULE__, {:submit_store, filename, file_path}, @default_timeout)
  end

  def read_file(filename) do
    GenServer.call(__MODULE__, {:submit_read, filename}, @default_timeout)
  end

  def delete_file(filename) do
    GenServer.call(__MODULE__, {:submit_delete, filename}, @default_timeout)
  end

  def init(init_arg) do
    {:ok, init_arg}
  end

  # ---------------------
  # GenServer Callbacks
  def handle_call({:submit_store, filename, file_path}, _from, state) do
    operation_id = "store_#{state.next_id}"

    task =
      Common.FileTask.new(%{
        id: operation_id,
        type: :store,
        filename: filename,
        file_path: file_path,
        started_at: DateTime.utc_now(),
        message: "Init storing"
      })

    task_struct = Task.async(fn -> execute_store_operation(task) end)
    done_task = Task.await(task_struct, @default_timeout)

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, done_task),
        next_id: state.next_id + 1
    }

    {:reply, {:ok, new_state}, new_state}
  end

  def handle_call({:submit_read, filename}, _from, state) do
    operation_id = "read_#{state.next_id}"

    task =
      Common.FileTask.new(%{
        id: operation_id,
        type: :read,
        filename: filename,
        file_path: nil,
        started_at: DateTime.utc_now(),
        message: "Init reading"
      })

    task_struct = Task.async(fn -> execute_read_operation(task) end)
    done_task = Task.await(task_struct, @default_timeout)

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, done_task),
        next_id: state.next_id + 1
    }

    {:reply, {:ok, done_task}, new_state}
  end

  def handle_call({:submit_delete, filename}, _from, state) do
    operation_id = "delete_#{state.next_id}"

    task =
      Common.FileTask.new(%{
        id: operation_id,
        type: :delete,
        filename: filename,
        file_path: nil,
        started_at: DateTime.utc_now(),
        message: "Init deleting"
      })

    task_struct = Task.async(fn -> execute_delete_operation(task) end)
    done_task = Task.await(task_struct, @default_timeout)

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, done_task),
        next_id: state.next_id + 1
    }

    {:reply, {:ok, done_task}, new_state}
  end

  def handle_call({:get_status, op_id}, _from, state) do
    {:reply, Map.get(state.operations, op_id), state}
  end

  # ---------------------
  # Task execution stubs
  defp execute_store_operation(task) do
    task = Common.FileTask.mark_running(task, "Calculating file size")
    file_size = File.stat!(task.file_path).size
    num_blocks = Common.Block.calculate_num_blocks(file_size)

    task = Common.FileTask.update_progress(task, 0, num_blocks, "Allocating blocks")
    block_size = Common.Block.get_block_size()

    result =
      File.stream!(task.file_path, block_size, [:read, :binary])
      |> Stream.with_index()
      |> Task.async_stream(
        fn {data, index} ->
          block_id = "#{task.filename}_block_#{index}"

          case wait_for_worker() do
            :no_worker_registered ->
              {:error, :no_worker}

            worker_info ->
              case GenServer.call(worker_info.pid, {:store_block, block_id, data}, @default_timeout) do
                updated_worker_state ->
                  # Register block -> worker mapping
                  _ = MiniHadoop.Master.MasterNode.register_block_worker(block_id, updated_worker_state)
                  _ = MiniHadoop.Master.MasterNode.update_tree(updated_worker_state)
                  {:ok, block_id}

                {:error, reason} ->
                  {:error, reason}
              end
          end
        end,
        max_concurrency: 10_000,
        ordered: true,
        timeout: :infinity
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, block_id}, {:ok, acc} ->
          {:cont, {:ok, [block_id | acc]}}

        {:error, reason}, _acc ->
          {:halt, {:error, reason}}
      end)

    # cek hasil akhir
    case result do
      {:ok, block_ids} ->
        # Register filename -> block_ids mapping
        # Since ordered: true, blocks come in order but we prepended, so reverse

        block_ids = block_ids |> Enum.reverse() |> Keyword.values()
        IO.inspect(block_ids, label: "block_ids")
        MiniHadoop.Master.MasterNode.register_file_blocks(task.filename, block_ids)
        Common.FileTask.mark_completed(task, "All #{num_blocks} blocks stored successfully")

      {:error, :no_worker} ->
        Common.FileTask.mark_failed(task, :no_worker, "No worker registered")

      {:error, reason} ->
        Common.FileTask.mark_failed(task, :error, "Failed: #{inspect(reason)}")
    end
  end

  defp wait_for_worker do
    case MiniHadoop.Master.MasterNode.pop_smallest() do
      {:error, :no_worker_registered} -> :no_worker_registered
      {:ok, worker} -> worker
    end
  end

  defp execute_read_operation(task) do
    task = Common.FileTask.mark_running(task, "Finding blocks for file")

    case MiniHadoop.Master.MasterNode.find_blocks_for_filename(task.filename) do
      {:ok, blocks_with_owners} ->
        num_blocks = length(blocks_with_owners)

        if num_blocks == 0 do
          Common.FileTask.mark_failed(task, :file_not_found, "No blocks found for file: #{task.filename}")
        else
          task = Common.FileTask.update_progress(task, 0, num_blocks, "Reading #{num_blocks} blocks")

          result =
            blocks_with_owners
            |> Task.async_stream(
              fn {_index, block_id, worker_info} ->
                case GenServer.call(worker_info.pid, {:read_block, block_id}, @default_timeout) do
                  {:ok, block_data} -> {:ok, {block_id, block_data}}
                  {:error, reason} -> {:error, {block_id, reason}}
                end
              end,
              max_concurrency: 10_000,
              ordered: true,
              timeout: :infinity
            )
            |> Enum.reduce_while({:ok, []}, fn
              {:ok, {block_id, block_data}}, {:ok, acc} ->
                {:cont, {:ok, [{block_id, block_data} | acc]}}

              {:error, {block_id, reason}}, _acc ->
                {:halt, {:error, {block_id, reason}}}
            end)

          case result do
            {:ok, blocks_data} ->
              # Since ordered: true, blocks come in order, but we prepended so reverse
              # Then sort by index to ensure correct order (in case of any issues)
              IO.inspect(blocks_data, label: "blocks_data")
              sorted_blocks =
                blocks_data
                |> Enum.reverse()
                |> Keyword.values()
                |> Enum.map(fn {block_id, data} ->
                  index =
                    block_id
                    |> String.replace("#{task.filename}_block_", "")
                    |> String.to_integer()

                  {index, data}
                end)
                |> Enum.sort_by(fn {index, _data} -> index end)
                |> Enum.map(fn {_index, data} -> data end)

              # Reconstruct file by concatenating all blocks
              file_content = IO.iodata_to_binary(sorted_blocks)

              # Write to default shared location
              default_path = Path.join("/shared", task.filename)
              :ok = File.mkdir_p("/shared")

              case File.write(default_path, file_content) do
                :ok ->
                  Common.FileTask.update_progress(
                    task,
                    num_blocks,
                    num_blocks,
                    "File reconstructed successfully"
                  )
                  |> Common.FileTask.mark_completed("File read and reconstructed: #{default_path}")

                {:error, reason} ->
                  Common.FileTask.mark_failed(task, reason, "Failed to write file: #{inspect(reason)}")
              end

            {:error, {block_id, reason}} ->
              Common.FileTask.mark_failed(
                task,
                reason,
                "Failed to read block #{block_id}: #{inspect(reason)}"
              )
          end
        end

      {:error, reason} ->
        Common.FileTask.mark_failed(task, reason, "Failed to find blocks: #{inspect(reason)}")
    end
  end

  defp execute_delete_operation(task) do
    task = Common.FileTask.mark_running(task, "Finding blocks for file")

    case MiniHadoop.Master.MasterNode.find_blocks_for_filename(task.filename) do
      {:ok, blocks_with_owners} ->
        num_blocks = length(blocks_with_owners)

        if num_blocks == 0 do
          Common.FileTask.mark_completed(task, "No blocks found for file: #{task.filename}")
        else
          task = Common.FileTask.update_progress(task, 0, num_blocks, "Deleting #{num_blocks} blocks")

          result =
            blocks_with_owners
            |> Task.async_stream(
              fn {_index, block_id, worker_info} ->
                updated_worker_state =
                  GenServer.call(worker_info.pid, {:delete_block, block_id}, @default_timeout)

                # Unregister block->worker mapping
                MiniHadoop.Master.MasterNode.unregister_block_worker(block_id)
                # Update master tree after deletion
                MiniHadoop.Master.MasterNode.update_tree(updated_worker_state)
                {:ok, block_id}
              end,
              max_concurrency: 10_000,
              ordered: false,
              timeout: :infinity
            )
            |> Enum.reduce_while(:ok, fn
              {:ok, _block_id}, acc ->
                {:cont, acc}

              {:error, {block_id, reason}}, _acc ->
                {:halt, {:error, {block_id, reason}}}
            end)

          # cek hasil akhir
          case result do
            :ok ->
              # Unregister filename->blocks mapping after all blocks are deleted
              MiniHadoop.Master.MasterNode.unregister_file_blocks(task.filename)
              IO.inspect(task.filename, label: "task.filename")
              Common.FileTask.update_progress(
                task,
                num_blocks,
                num_blocks,
                "All blocks deleted successfully"
              )
              |> Common.FileTask.mark_completed("All #{num_blocks} blocks deleted successfully")

            {:error, {block_id, reason}} ->
              Common.FileTask.mark_failed(
                task,
                reason,
                "Failed to delete block #{block_id}: #{inspect(reason)}"
              )
          end
        end

      {:error, reason} ->
        Common.FileTask.mark_failed(task, reason, "Failed to find blocks: #{inspect(reason)}")
    end
  end
end
