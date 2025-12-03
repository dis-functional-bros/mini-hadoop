defmodule MiniHadoop.Master.FileOperation do
  use GenServer
  require Logger
  alias MiniHadoop.Models.FileTask
  alias MiniHadoop.Models.Block
  alias MiniHadoop.Master.MasterNode

  @default_timeout 3_000_000
  @batch_size Application.get_env(:mini_hadoop, :batch_size, 100)
  @replication_factor Application.get_env(:mini_hadoop, :block_replication_factor, 2)


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

  def get_operation_info(task_id) do
    GenServer.call(__MODULE__, {:get_operation_info, task_id}, @default_timeout)
  end

  def init(_) do
    retrieve_result_path = Application.get_env(:mini_hadoop, :retrieve_result_path)
    {:ok, %{operations: %{}, next_id: 1, retrieve_result_path: retrieve_result_path}}
  end

  def handle_cast({:update_operation, operation_id, operation_new_state}, state) do
    new_state = %{state | operations: Map.put(state.operations, operation_id, operation_new_state)}
    {:noreply, new_state}
  end

  def handle_call({:get_operation_info, operation_id}, _from, state) do
    {:reply, Map.get(state.operations, operation_id), state}
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
        create_and_start_operation(:store, filename, file_path, state)
    end
  end

  def handle_call({:submit_retrieve, filename}, _from, state) do
    cond do
      filename == "" ->
        {:reply, {:error, "Filename cannot be empty"}, state}
      !MasterNode.filename_exists(filename) ->
        {:reply, {:error, "File does not exist"}, state}
      true ->
        create_and_start_operation(:retrieve, filename, nil, state)
    end
  end

  def handle_call({:submit_delete, filename}, _from, state) do
    cond do
      filename == "" ->
        {:reply, {:error, "Filename cannot be empty"}, state}
      !MasterNode.filename_exists(filename) ->
        {:reply, {:error, "File does not exist"}, state}
      true ->
        create_and_start_operation(:delete, filename, nil, state)
    end
  end

  # Operation Creation Helper
  defp create_and_start_operation(type, filename, file_path, state) do
    operation_id = "#{type}_#{state.next_id}"

    task = FileTask.new(%{
      id: operation_id,
      type: type,
      filename: filename,
      file_path: file_path
    })
    |> FileTask.mark_started("Initializing #{type} operation")

    new_state = %{
      state
      | operations: Map.put(state.operations, operation_id, task),
        next_id: state.next_id + 1
    }

    Task.start(fn ->
      case type do
        :store -> execute_store_operation(task)
        :retrieve -> execute_retrieve_operation(task, new_state.retrieve_result_path)
        :delete -> execute_delete_operation(task)
      end
    end)

    {:reply, {:ok, %{operation_id: operation_id, status: :started, task: task}}, new_state}
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
    task = FileTask.mark_running(task, "Calculating file size")
    file_size = File.stat!(task.file_path).size
    num_blocks = Block.calculate_num_blocks(file_size)
    total_operations = num_blocks * @replication_factor

    task = FileTask.update_progress(task, 0, total_operations, "Allocating blocks")
    block_size = Block.get_block_size()
    IO.puts("Number of blocks: #{num_blocks}")

    # Send initial state update via cast (async)
    GenServer.cast(__MODULE__, {:update_operation, task.id, task})

    result =
      File.stream!(task.file_path, block_size, [:read, :binary])
      |> Stream.with_index()
      |> Stream.chunk_every(@batch_size)
      |> Enum.reduce_while({:ok, [], 0}, fn chunk, {status, acc_blocks, processed_count} ->
        case status do
          {:error, _} -> {:halt, {status, acc_blocks, processed_count}}
          _ ->
            chunk_result =
              chunk
              |> Task.async_stream(
                fn {data, index} ->
                  block_id = "#{task.filename}_block_#{index}"

                  # Sequential replication
                  success_count =
                    Enum.reduce_while(1..@replication_factor, 0, fn attempt, count ->
                      case wait_for_worker(block_id) do
                        :no_worker_registered ->
                          {:cont, count}
                        worker_pid ->
                          case GenServer.call(worker_pid, {:store_block, block_id, data}, @default_timeout) do
                            {:store, updated_worker_state} ->
                              MasterNode.update_tree({:store, updated_worker_state})
                              IO.puts("Block stored successfully")
                              if count + 1 >= @replication_factor,
                                do: {:halt, count + 1},
                                else: {:cont, count + 1}
                            _ ->
                              {:cont, count}
                          end
                      end
                    end)

                  if success_count >= @replication_factor,
                    do: {:ok, {index, block_id}},
                    else: {:error, :insufficient_replication}
                end,
                max_concurrency: 10_000,
                ordered: true,
                timeout: :infinity
              )
              |> Enum.reduce_while({:ok, []}, fn
                {:ok, {:ok, block_data}}, {:ok, acc} -> {:cont, {:ok, [block_data | acc]}}
                {:ok, {:error, reason}}, _acc -> {:halt, {:error, reason}}
                {:exit, reason}, _acc -> {:halt, {:error, reason}}
              end)

              case chunk_result do
                {:ok, chunk_blocks} ->
                  sorted_chunk_blocks =
                    chunk_blocks
                    |> Enum.sort_by(fn {index, _block_id} -> index end)

                  new_processed_count = processed_count + (length(chunk) * @replication_factor)
                  updated_task = FileTask.update_progress(
                    task,
                    new_processed_count,
                    total_operations,
                    "Processed #{new_processed_count}/#{total_operations} operations"
                  )

                  # Send async update to avoid deadlock

                  GenServer.cast(__MODULE__, {:update_operation, task.id, updated_task})

                  # Prepend sorted chunk for O(1) performance
                  {:cont, {:ok, [sorted_chunk_blocks | acc_blocks], new_processed_count}}

                {:error, reason} ->
                  failed_task = FileTask.mark_failed(task, reason, "Failed: #{inspect(reason)}")
                  GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
                  {:halt, {{:error, reason}, acc_blocks, processed_count}}
              end
        end
      end)

    case result do
      {{:error, reason}, _blocks, _processed} ->

        {:error, reason}

      {:ok, reversed_chunk_list, final_processed} ->
        # Reverse chunks to get correct order, then flatten
        sorted_block_ids_with_index =
          reversed_chunk_list
          |> Enum.reverse()
          |> List.flatten()

        sorted_block_ids = Enum.map(sorted_block_ids_with_index, fn {_index, block_id} -> block_id end)


        task_with_final_progress = FileTask.update_progress(
          task,
          total_operations,
          total_operations,
          "All #{total_operations} blocks stored successfully"
        )

        completed_task = FileTask.mark_completed(
          task_with_final_progress,
          "All #{total_operations} blocks stored successfully"
        )

        MiniHadoop.Master.MasterNode.register_file_blocks(task.filename, sorted_block_ids)

        GenServer.cast(__MODULE__, {:update_operation, task.id, completed_task})
        {:ok, completed_task}
    end
  end

  defp execute_retrieve_operation(task, retrieve_result_path) do
    task = FileTask.mark_running(task, "Finding blocks for file")
    GenServer.cast(__MODULE__, {:update_operation, task.id, task})

    case MiniHadoop.Master.MasterNode.get_blocks_assingment_for_file(task.filename) do
      {:ok, blocks_with_owners} ->
        num_blocks = length(blocks_with_owners)

        if num_blocks == 0 do
          failed_task = FileTask.mark_failed(task, :file_not_found, "No blocks found for file: #{task.filename}")
          GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
          failed_task
        else
          task = FileTask.update_progress(task, 0, num_blocks, "Retrieving #{num_blocks} blocks")
          GenServer.cast(__MODULE__, {:update_operation, task.id, task})

          default_path = Path.join(retrieve_result_path, task.filename)
          :ok = File.mkdir_p(retrieve_result_path)

          {:ok, file_handle} = File.open(default_path, [:write, :raw, :binary])

          result =
            blocks_with_owners
            |> Task.async_stream(
              fn {index, block_id, [first_worker_pid | _]} ->
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

                #
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
                      updated_task = FileTask.update_progress(
                        task,
                        new_count,
                        num_blocks,
                        "Processed #{new_count}/#{num_blocks} blocks"
                      )
                      GenServer.cast(__MODULE__, {:update_operation, task.id, updated_task})

                      {:cont, {:ok, [], new_count}}

                    {:error, reason} ->
                      IO.inspect("Error writing batch: #{reason}")
                      failed_task = FileTask.mark_failed(task, :write_error, "Failed to write batch: #{inspect(reason)}")
                      GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
                      {:halt, {:error, {:write_failed, reason}}}
                  end
                else
                  # Continue accumulating blocks in buffer
                  {:cont, {:ok, new_buffer, processed_count}}
                end

              {:error, {block_id, reason}}, _acc ->
                failed_task = FileTask.mark_failed(task, reason, "Failed to retrieve block #{block_id}: #{inspect(reason)}")
                GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
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

          :ok = File.close(file_handle)

          case final_result do
            {:ok, processed_count} ->
              if processed_count == num_blocks do
                task_with_final_progress = FileTask.update_progress(
                  task,
                  num_blocks,
                  num_blocks,
                  "All #{num_blocks} blocks retrieved successfully"
                )

                completed_task = FileTask.mark_completed(task, "File reconstructed successfully: #{default_path}")
                GenServer.cast(__MODULE__, {:update_operation, task.id, completed_task})
                completed_task
              else
                # Partial success
                completed_task = FileTask.mark_completed(task, "File partially reconstructed (#{processed_count}/#{num_blocks} blocks): #{default_path}")
                GenServer.cast(__MODULE__, {:update_operation, task.id, completed_task})
                completed_task
              end

            {:error, {:write_failed, reason}} ->
              # Clean up partial file on write error
              File.rm(default_path)
              failed_task = FileTask.mark_failed(
                task,
                reason,
                "Failed to write file: #{inspect(reason)}"
              )
              GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
              failed_task

            {:error, {block_id, reason}} ->
              # Clean up partial file on retrieval error
              File.rm(default_path)
              failed_task = FileTask.mark_failed(
                task,
                reason,
                "Failed to retrieve block #{block_id}: #{inspect(reason)}"
              )
              GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
              failed_task
          end
        end

      {:error, :file_not_found} ->
        failed_task = FileTask.mark_failed(task, :file_not_found, "File not found: #{task.filename}")
        GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
        failed_task

      {:error, reason} ->
        failed_task = FileTask.mark_failed(task, reason, "Failed to find blocks: #{inspect(reason)}")
        GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
        failed_task
    end
  end

  defp execute_delete_operation(task) do
    task = FileTask.mark_running(task, "Finding blocks for file deletion")
    GenServer.cast(__MODULE__, {:update_operation, task.id, task})

    case MiniHadoop.Master.MasterNode.get_blocks_assingment_for_file(task.filename) do
      {:ok, blocks_with_owners} ->
        num_blocks = length(blocks_with_owners) * @replication_factor

        if num_blocks == 0 do
          completed_task = FileTask.mark_completed(task, "No blocks found for file: #{task.filename}")
          GenServer.cast(__MODULE__, {:update_operation, task.id, completed_task})
          completed_task
        else
          task = FileTask.update_progress(task, 0, num_blocks, "Deleting #{num_blocks} blocks")
          GenServer.cast(__MODULE__, {:update_operation, task.id, task})

          result =
            blocks_with_owners
            |> Stream.chunk_every(@batch_size)  # Process in batches
            |> Enum.reduce_while({:ok, 0}, fn chunk, {status, processed_count} ->
              case status do
                {:error, _} -> {:halt, {status, processed_count}}
                _ ->
                  # Process current chunk
                  chunk_result =
                    chunk
                    |> Task.async_stream(
                      fn {_index, block_id, worker_pids} ->
                        {failed_deletion, success_count} =
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
                    |> Enum.reduce_while({:ok, []}, fn
                      {:ok, {:ok, block_id}}, {:ok, acc} -> {:cont, {:ok, [block_id | acc]}}
                      {:ok, {:error, reason}}, _acc -> {:halt, {:error, reason}}
                      {:exit, reason}, _acc -> {:halt, {:error, reason}}
                    end)

                  case chunk_result do
                    {:ok, _deleted_blocks} ->
                      # Update progress after each chunk
                      new_processed_count = processed_count + (length(chunk) * @replication_factor)
                      updated_task = FileTask.update_progress(
                        task,
                        new_processed_count,
                        num_blocks,
                        "Processed #{new_processed_count}/#{num_blocks} operations"
                      )

                      # Send async update
                      GenServer.cast(__MODULE__, {:update_operation, task.id, updated_task})

                      {:cont, {:ok, new_processed_count}}

                    {:error, reason} ->
                      failed_task = FileTask.mark_failed(task, reason, "Failed: #{inspect(reason)}")
                      GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
                      {:halt, {{:error, reason}, processed_count}}
                  end
              end
            end)

          case result do
            {{:error, reason}, _processed_count} ->
              {:error, reason}

            {:ok, final_processed} ->
              MasterNode.rebuild_tree_after_deletion()
              MasterNode.unregister_file_blocks(task.filename)

              task_with_final_progress = FileTask.update_progress(
                task,
                num_blocks,
                num_blocks,
                "All #{num_blocks} blocks deleted successfully"
              )

              completed_task = FileTask.mark_completed(
                task_with_final_progress,
                "All #{num_blocks} blocks deleted successfully"
              )

              GenServer.cast(__MODULE__, {:update_operation, task.id, completed_task})
              {:ok, completed_task}
          end
        end

      {:error, :file_not_found} ->
        failed_task = FileTask.mark_failed(task, :file_not_found, "File not found: #{task.filename}")
        GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
        failed_task

      {:error, reason} ->
        failed_task = FileTask.mark_failed(task, reason, "Failed to find blocks: #{inspect(reason)}")
        GenServer.cast(__MODULE__, {:update_operation, task.id, failed_task})
        failed_task
    end
  end

end
