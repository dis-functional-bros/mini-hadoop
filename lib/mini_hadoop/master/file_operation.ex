defmodule MiniHadoop.Master.FileOperation do
  use GenServer
  require Logger
  alias MiniHadoop.Common
  alias MiniHadoop.Master.MasterNode

  @default_timeout 3_000_000
  @batch_size 100
  @replication_factor 2

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{operations: %{}, next_id: 1}, name: __MODULE__)
  end

  # ---------------------
  # Sync operations
  def store_file(filename, file_path) do
    GenServer.call(__MODULE__, {:submit_store, filename, file_path}, @default_timeout)
  end

  def retrieve_file(filename) do
    GenServer.call(__MODULE__, {:submit_retrieve, filename}, @default_timeout)
  end

  def delete_file(filename) do
    GenServer.call(__MODULE__, {:submit_delete, filename}, @default_timeout)
  end

  def get_status(task_id) do
    GenServer.call(__MODULE__, {:get_status, task_id}, @default_timeout)
  end

  def init(init_arg) do
    {:ok, init_arg}
  end

  # ---------------------
  # GenServer Callbacks
  def handle_call({:submit_store, filename, file_path}, _from, state) do
    cond do
      !File.exists?(file_path) ->
        {:reply, {:error, "File not found"}, state}

      MasterNode.filename_exists(filename) ->
        {:reply, {:error, "Filename already in use"}, state}

      true ->
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
  end

  def handle_call({:submit_retrieve, filename}, _from, state) do
    cond do
      filename == "" ->
        {:reply, {:error, "Filename cannot be empty"}, state}

      !MasterNode.filename_exists(filename) ->
        {:reply, {:error, "File does not exist"}, state}

      true ->
        operation_id = "retrieve_#{state.next_id}"

        task =
          Common.FileTask.new(%{
            id: operation_id,
            type: :retrieve,
            filename: filename,
            file_path: nil,
            started_at: DateTime.utc_now(),
            message: "Init retrieve #{filename}"
          })

        task_struct = Task.async(fn -> execute_retrieve_operation(task) end)
        done_task = Task.await(task_struct, @default_timeout)

        new_state = %{
          state
          | operations: Map.put(state.operations, operation_id, done_task),
            next_id: state.next_id + 1
        }

        {:reply, {:ok, done_task}, new_state}
    end
  end

  def handle_call({:submit_delete, filename}, _from, state) do
    cond do
      filename == "" ->
        {:reply, {:error, "Filename cannot be empty"}, state}

      !MasterNode.filename_exists(filename) ->
        {:reply, {:error, "File does not exist"}, state}

      true ->
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
  end

  def handle_call({:get_status, op_id}, _from, state) do
    {:reply, Map.get(state.operations, op_id), state}
  end

  defp wait_for_worker(block_id) do
    case MasterNode.pop_smallest(block_id) do
      {:error, :no_worker_registered} -> :no_worker_registered
      {:ok, worker} -> worker
    end
  end

  # ---------------------
  # Task execution stubs
  defp execute_store_operation(task) do
    task = Common.FileTask.mark_running(task, "Calculating file size")
    file_size = File.stat!(task.file_path).size
    num_blocks = Common.Block.calculate_num_blocks(file_size) * @replication_factor

    task = Common.FileTask.update_progress(task, 0, num_blocks * @replication_factor, "Allocating blocks")
    block_size = Common.Block.get_block_size()
    IO.puts("Number of blocks: #{num_blocks}")

    result =
      File.stream!(task.file_path, block_size, [:read, :binary])
      |> Stream.with_index()
      |> Task.async_stream(
        fn {data, index} ->
          block_id = "#{task.filename}_block_#{index}"

          # Sequential replication instead of parallel
          success_count =
            Enum.reduce_while(1..@replication_factor, 0, fn attempt, count ->
              case wait_for_worker(block_id) do
                :no_worker_registered ->
                  {:cont, count}  # Try next attempt
                worker_pid ->
                  case GenServer.call(worker_pid, {:store_block, block_id, data}, @default_timeout) do
                    {:store, updated_worker_state} ->
                      MasterNode.update_tree({:store, updated_worker_state})
                      if count + 1 >= @replication_factor,
                        do: {:halt, count + 1},
                        else: {:cont, count + 1}
                    _ ->
                      {:cont, count}  # Try next attempt
                  end
              end
            end)

          if success_count >= @replication_factor,
            do: {:ok, block_id},
            else: {:error, :insufficient_replication}
        end,
        max_concurrency: 4,  # Match your worker count
        ordered: true,
        timeout: :infinity
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {:ok, block_id}}, {:ok, acc} -> {:cont, {:ok, [block_id | acc]}}
        {:ok, {:error, reason}}, _acc -> {:halt, {:error, reason}}
        {:exit, reason}, _acc -> {:halt, {:error, reason}}
      end)

    # Check final result
    case result do
      {:ok, block_ids} ->
        block_ids = block_ids |> Enum.reverse()
        MiniHadoop.Master.MasterNode.register_file_blocks(task.filename, block_ids)
        Common.FileTask.mark_completed(task, "All #{num_blocks} blocks stored successfully")

      {:error, :no_worker} ->
        Common.FileTask.mark_failed(task, :no_worker, "No worker registered")

      {:error, reason} ->
        Common.FileTask.mark_failed(task, :error, "Failed: #{inspect(reason)}")
    end
  end

  defp execute_retrieve_operation(task) do
    task = Common.FileTask.mark_running(task, "Finding blocks for file")

    case MiniHadoop.Master.MasterNode.get_blocks_assingment_for_file(task.filename) do
      {:ok, blocks_with_owners} ->
        num_blocks = length(blocks_with_owners)

        if num_blocks == 0 do
          Common.FileTask.mark_failed(task, :file_not_found, "No blocks found for file: #{task.filename}")
        else
          task = Common.FileTask.update_progress(task, 0, num_blocks, "Retrieving #{num_blocks} blocks")

          # Create output file path
          default_path = Path.join("/shared", task.filename)
          :ok = File.mkdir_p("/shared")

          # OPEN FILE ONCE for streaming write
          {:ok, file_handle} = File.open(default_path, [:write, :raw, :binary])

          result =
            blocks_with_owners
            |> Task.async_stream(
              fn {index, block_id, [first_worker_pid, _ ]} ->
                case GenServer.call(first_worker_pid, {:retrieve_block, block_id}, @default_timeout) do
                  {:ok, block_data} ->
                    {:ok, {index, block_data}}
                  {:error, reason} ->
                    IO.inspect("Error retrieving block #{block_id}: #{reason}")
                    {:error, {block_id, reason}}
                end
              end,
              max_concurrency: 10_000,
              ordered: true,
              timeout: :infinity
            )
            |> Enum.reduce_while({:ok, [], 0}, fn
              {:ok, {index, block_data}}, {:ok, buffer, processed_count} ->
                # Add block to buffer
                new_buffer = [{index, block_data} | buffer]

                # Check if we reached batch size
                if length(new_buffer) >= @batch_size do
                  # Sort blocks by index to maintain correct order
                  sorted_batch =
                    new_buffer
                    |> Enum.reverse()
                    |> Enum.map(fn {_, {_, data}} -> data end)
                    |> IO.iodata_to_binary()

                  # Write the entire batch
                  case :file.write(file_handle, sorted_batch) do
                    :ok ->
                      new_count = processed_count + length(new_buffer)
                      Common.FileTask.update_progress(task, new_count, num_blocks, "Processed #{new_count}/#{num_blocks} blocks")
                      # Reset buffer and continue
                      {:cont, {:ok, [], new_count}}

                    {:error, reason} ->
                      IO.inspect("Error writing batch: #{reason}")
                      {:halt, {:error, {:write_failed, reason}}}
                  end
                else
                  # Continue accumulating blocks in buffer
                  {:cont, {:ok, new_buffer, processed_count}}
                end

              {:error, {block_id, reason}}, _acc ->
                {:halt, {:error, {block_id, reason}}}
            end)

          # Handle any remaining blocks in buffer after processing all streams
          final_result = case result do
            {:ok, remaining_buffer, processed_count} when remaining_buffer != [] ->
              # Write final partial batch
              sorted_batch =
                remaining_buffer
                |> Enum.reverse()
                |> Enum.map(fn {_, {_, data}} -> data end)
                |> IO.iodata_to_binary()

              case :file.write(file_handle, sorted_batch) do
                :ok ->
                  final_count = processed_count + length(remaining_buffer)
                  {:ok, final_count}

                {:error, reason} ->
                  {:error, {:write_failed, reason}}
              end

            {:ok, _, processed_count} ->
              {:ok, processed_count}

            error ->
              error
          end

          # CLOSE FILE regardless of result
          :ok = File.close(file_handle)

          case final_result do
            {:ok, processed_count} ->
              if processed_count == num_blocks do
                Common.FileTask.mark_completed(task, "File reconstructed successfully: #{default_path}")
              else
                # Partial success - some blocks might have been written
                Common.FileTask.mark_completed(task, "File partially reconstructed (#{processed_count}/#{num_blocks} blocks): #{default_path}")
              end


            {:error, {:write_failed, reason}} ->
              # Clean up partial file on write error
              File.rm(default_path)
              Common.FileTask.mark_failed(
                task,
                reason,
                "Failed to write file: #{inspect(reason)}"
              )
          end

        end

      {:error, :file_not_found} ->
        Common.FileTask.mark_failed(task, :file_not_found, "File not found : #{inspect(task.filename)}")

      {:error, reason} ->
        Common.FileTask.mark_failed(task, reason, "Failed to find blocks: #{inspect(reason)}")
    end
  end

  defp execute_delete_operation(task) do
    case MiniHadoop.Master.MasterNode.get_blocks_assingment_for_file(task.filename) do
      {:ok, blocks_with_owners} ->
        num_blocks = length(blocks_with_owners) * @replication_factor

        if num_blocks == 0 do
          Common.FileTask.mark_completed(task, "No blocks found for file: #{task.filename}")
        else
          task = Common.FileTask.update_progress(task, 0, num_blocks, "Deleting #{num_blocks} blocks")

          result =
            blocks_with_owners
            |> Task.async_stream(
              fn {_index, block_id, worker_pids} ->

                {failed_deletion, _} =
                  Enum.reduce_while(worker_pids, {nil, 0}, fn worker_pid, {failed, success_count} ->
                    case GenServer.call(worker_pid, {:delete_block, block_id}, @default_timeout) do
                      {operation, updated_worker_state} when operation in [:store, :delete] ->
                        MasterNode.update_tree({operation, updated_worker_state})
                        {:cont, {failed, success_count + 1}}
                      {:error, reason} ->
                        {:halt, {{block_id, reason}, success_count}}
                      unexpected ->
                        {:halt, {{block_id, {:unexpected_response, unexpected}}, success_count}}
                    end
                  end)

                case failed_deletion do
                  nil -> {:ok, block_id}
                  error -> {:error, error}
                end
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
          # Handle final result
          case result do
            :ok ->
              MasterNode.rebuild_tree_after_deletion()
              MasterNode.unregister_file_blocks(task.filename)
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

      {:error, :file_not_found} ->
        Common.FileTask.mark_failed(task, :file_not_found, "File not found : #{inspect(task.filename)}")

      {:error, reason} ->
        Common.FileTask.mark_failed(task, reason, "Failed to find blocks: #{inspect(reason)}")
    end
  end
end
