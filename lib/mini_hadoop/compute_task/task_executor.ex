defmodule MiniHadoop.ComputeTask.TaskExecutor do
  @moduledoc """
  Provides a monadic execution pipeline for MapReduce tasks.

  This module is designed to handle potentially large datasets (e.g., 64MB+ map tasks)
  by implementing aggressive memory management strategies, such as immediate data clearing
  and forced garbage collection after heavy operations.
  """
  require Logger

  alias MiniHadoop.Models.ComputeTask

  @type execution_result :: {:ok, ComputeTask.t()} | {:error, term()}
  @large_data_threshold 64_000_000  # 64MB - trigger aggressive memory management

  # -- Monadic Wrappers --
  @doc false
  def pure(task), do: {:ok, task}

  @doc false
  def error(reason), do: {:error, reason}
  def bind({:ok, task}, func), do: func.(task)
  def bind({:error, reason}, _func), do: {:error, reason}

  @doc """
  Executes a compute task through a memory-optimized pipeline.

  Steps:
  1. Fetch data
  2. Execute user function
  3. Normalize result
  4. Mark complete

  Triggers garbage collection before returning to keep memory footprint low.
  """
  @spec execute_task(ComputeTask.t()) :: execution_result()
  def execute_task(task) do
    result =
      pure(task)
      |> bind(&fetch_task_data/1)
      |> bind(&execute_user_function/1)
      |> bind(&normalize_user_result/1)
      |> bind(&mark_task_completed/1)

    # Force garbage collection to ensure immediate reclamation of large binaries
    # allocated during the task execution.
    :erlang.garbage_collect(self())

    result
  end

  # Pipeline steps with memory optimization
  defp fetch_task_data(task) do
    case task.type do
      :map -> fetch_map_data_memory_optimized(task)
      :reduce -> fetch_reduce_data_memory_optimized(task)
    end
  end

  # Fetch map data with minimal allocations
  defp fetch_map_data_memory_optimized(%{input_data: {block_id, worker_pids}} = task) do
    case fetch_block_with_fallback(block_id, worker_pids) do
      {:ok, data} ->
        # Optimization: Clear `input_data` reference immediately to free memory
        # if the garbage collector runs.
        task = %{task | data: data, input_data: :cleared}

        if is_binary(data) and byte_size(data) > 64_000_000 do
          :erlang.garbage_collect(self())
        end

        {:ok, task}

      {:error, reason} ->
        {:error, {:block_fetch_failed, reason}}
    end
  end

  defp fetch_block_with_fallback(block_id, [worker_pid | rest]) do
    try do
      case GenServer.call(worker_pid, {:retrieve_block, block_id}, 15_000) do
        {:ok, data} -> {:ok, data}
        _ -> fetch_block_with_fallback(block_id, rest)
      end
    rescue
      _ -> fetch_block_with_fallback(block_id, rest)
    end
  end

  defp fetch_block_with_fallback(_block_id, []), do: {:error, :all_workers_failed}

  # Fetch reduce data with streaming
  defp fetch_reduce_data_memory_optimized(task) do
    # task.input_data is now: {start_key, end_key, storage_pids}
    case fetch_reduce_values_streaming(task.input_data) do
      {:ok, values} ->
        # Clear input_data immediately
        {:ok, %{task | data: values, input_data: :cleared}}

      {:error, reason} ->
        {:error, {:data_fetch_failed, reason}}
    end
  end

  defp fetch_reduce_values_streaming({start_key, end_key, storage_pids}) do
    # Fetch ALL keys in the range from ALL storage processes
    result =
      storage_pids
      |> Task.async_stream(
        fn storage_pid ->
          # Ask each storage for ALL keys in the range
          case GenServer.call(storage_pid, {:get_data_in_range, start_key, end_key}, 30_000) do
            {:ok, key_values_map} -> key_values_map
            _ -> %{}
          end
        end,
        max_concurrency: length(storage_pids),
        timeout: 60_000
      )
      |> Enum.reduce(%{}, fn
        {:ok, key_values_map}, acc ->
          # Merge results from different storages
          Map.merge(acc, key_values_map, fn _key, values1, values2 ->
            values1 ++ values2  # Combine values from different storages
          end)
        {:exit, _reason}, acc ->
          # Skip failed storages
          acc
      end)

    {:ok, result}
  end


  # Execute user function while monitoring memory usage
  defp execute_user_function(%{data: task_data, function: user_func, context: context} = task) do
    data_size = if is_binary(task_data), do: byte_size(task_data), else: 0

    try do
      # Execute the user function
      raw_result = user_func.(task_data, context)

      # Optimization: Clear the input `data` immediately after use.
      task = %{task | raw_result: raw_result, data: :cleared}

      if data_size > @large_data_threshold do
        :erlang.garbage_collect(self())
      end

      {:ok, task}
    rescue
      error ->
        Logger.error("User function crashed: #{Exception.message(error)}")
        # Still clear data on error
        {:error, {:user_function_crashed, Exception.message(error)}}
    catch
      kind, reason ->
        Logger.error("User function threw #{kind}: #{inspect(reason)}")
        # Still clear data on error
        {:error, {:user_function_threw, kind, reason}}
    end
  end

  # Normalize result with minimal allocations
  defp normalize_user_result(%{raw_result: raw_result} = task) do
    case validate_and_normalize(raw_result) do
      {:ok, normalized} ->
        # Clear raw_result immediately
        {:ok, %{task | normalized_result: normalized, raw_result: :cleared}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_and_normalize(raw_result) do
    case raw_result do
      {:ok, result} when is_list(result) ->
        if valid_kv_list_fast?(result) do
          {:ok, result}
        else
          {:error, {:invalid_result_format, "Expected list of {key, value} tuples"}}
        end

      {:error, reason} ->
        {:error, {:user_function_error, reason}}

      result when is_list(result) ->
        if valid_kv_list_fast?(result) do
          {:ok, result}
        else
          {:error, {:invalid_result_format, "Expected list of {key, value} tuples"}}
        end

      other ->
        {:error, {:unexpected_return_type, other}}
    end
  end

  # Faster validation for large lists
  defp valid_kv_list_fast?([]) do
    true
  end

  defp valid_kv_list_fast?([{key, _value} | rest]) when not is_nil(key) do
    valid_kv_list_fast?(rest)
  end

  defp valid_kv_list_fast?(_) do
    false
  end

  # Finalize task and release all data references
  defp mark_task_completed(%{normalized_result: result} = task) do
    completed_task = ComputeTask.mark_completed(task, result)

    # Optimization: Nullify all data-heavy fields in the returned struct.
    cleaned_task = %{completed_task |
      data: nil,
      raw_result: nil,
      normalized_result: nil,
      context: %{}
    }

    {:ok, cleaned_task}
  end

  @doc """
  Fetches values for a list of keys from distributed storage in batches.

  This helper manages memory pressure by processing keys in smaller chunks
  rather than loading all values into memory at once.
  """
  def fetch_all_value_of_keys(input) do
    # Process in very small batches for memory efficiency
    batch_size = max(div(length(input), 10), 10)

    input
    |> Enum.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, %{}}, fn batch, {:ok, acc} ->
      case process_key_batch_memory_safe(batch) do
        {:ok, batch_result} ->
          merged = Map.merge(acc, batch_result)
          {:cont, {:ok, merged}}
        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp process_key_batch_memory_safe(batch) do
    # Group by storage to minimize calls
    storage_to_keys = Enum.reduce(batch, %{}, fn {key, storage_pids}, acc ->
      Enum.reduce(storage_pids, acc, fn storage_pid, inner_acc ->
        Map.update(inner_acc, storage_pid, [key], &[key | &1])
      end)
    end)

    # Process storage calls with limited concurrency
    Logger.info("keys #{inspect(storage_to_keys)}")
    results =
      storage_to_keys
      |> Task.async_stream(fn {storage_pid, keys} ->
          try do
            case GenServer.call(storage_pid, {:get_values_of_keys, keys}, :infinity) do
              {:ok, values_map} -> {:ok, storage_pid, values_map}
              {:error, reason} -> {:error, storage_pid, reason}
            end
          rescue
            error -> {:error, storage_pid, error}
          end
        end,
        max_concurrency: min(10, map_size(storage_to_keys)),  # Low concurrency for memory
        timeout: :infinity
      )
      |> Enum.reduce_while(%{}, fn
        {:ok, {:ok, storage_pid, values_map}}, acc ->
          {:cont, Map.put(acc, storage_pid, values_map)}
        {:ok, {:error, storage_pid, reason}}, _acc ->
          {:halt, {:error, "Storage #{inspect(storage_pid)} failed: #{inspect(reason)}"}}
        {:exit, reason}, _acc ->
          {:halt, {:error, "Task failed: #{inspect(reason)}"}}
      end)

    case results do
      {:error, reason} -> {:error, reason}
      storage_results ->
        # Build result map
        result_map = Enum.reduce(batch, %{}, fn {key, storage_pids}, acc ->
          values = Enum.flat_map(storage_pids, fn storage_pid ->
            Map.get(storage_results, storage_pid, %{})
            |> Map.get(key, [])
          end)
          Map.put(acc, key, values)
        end)
        {:ok, result_map}
    end
  end
end
