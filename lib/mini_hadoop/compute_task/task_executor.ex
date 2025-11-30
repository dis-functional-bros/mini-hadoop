defmodule MiniHadoop.ComputeTask.TaskExecutor do
  @moduledoc """
  Monadic task executor for MapReduce tasks.
  Handles task execution with proper error handling and normalization.
  """
  require Logger

  alias MiniHadoop.Models.ComputeTask

  @type execution_result :: {:ok, ComputeTask.t()} | {:error, term()}

  # Monad operations
  def pure(task), do: {:ok, task}
  def error(reason), do: {:error, reason}

  # Bind operation for chaining monadic operations
  def bind({:ok, task}, func), do: func.(task)
  def bind({:error, reason}, _func), do: {:error, reason}

  @doc """
  Task execution pipeline.
  """
  @spec execute_task(ComputeTask.t()) :: execution_result()
  def execute_task(task) do
    pure(task)
    # |> bind(&simulate_processing_time/1)
    |> bind(&fetch_task_data/1)
    |> bind(&execute_user_function/1)
    |> bind(&normalize_user_result/1)
    |> bind(&mark_task_completed/1)
  end

  # Monadic pipeline steps
  defp simulate_processing_time(task) do
    Process.sleep(:rand.uniform(5000) + 50)
    pure(task)
  end

  defp fetch_task_data(task) do
    case task.type do
      :map -> fetch_map_data(task)
      :reduce -> fetch_reduce_data(task)
    end
  end

  defp fetch_map_data(task) do
    # TODO: Replace with actual implementation
    dummy_data = """
    To be or not to be, that is the question: Whether 'tis nobler in the mind to suffer the slings and arrows of outrageous fortune, or to take arms against a sea of troubles and by opposing end them.
    The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump! The five boxing wizards jump quickly.
    """

    pure(%{task | data: dummy_data})

    # Future implementation:
    # case fetch_block_data(task.input_data) do
    #   {:ok, data} -> pure(%{task | data: data})
    #   {:error, reason} -> error({:data_fetch_failed, reason})
    # end
  end

  defp fetch_reduce_data(task) do
    case fetch_all_value_of_keys(task.input_data) do
      {:ok, values} -> pure(%{task | data: values})
      {:error, reason} -> error({:data_fetch_failed, reason})
    end
  end

  defp execute_user_function(%{data: task_data, function: user_func, context: context} = task) do
    try do
      raw_result = user_func.(task_data, context)
      pure(%{task | raw_result: raw_result})
    rescue
      error ->
        Logger.error("User function crashed: #{Exception.message(error)}")
        error({:user_function_crashed, Exception.message(error)})
    catch
      kind, reason ->
        Logger.error("User function threw #{kind}: #{inspect(reason)}")
        error({:user_function_threw, kind, reason})
    end
  end

  defp normalize_user_result(%{raw_result: raw_result} = task) do
    case raw_result do
      # Standard contract format
      {:ok, result} when is_list(result) ->
        if valid_kv_list?(result) do
          pure(%{task | normalized_result: result})
        else
          error({:invalid_result_format, "Expected list of {key, value} tuples"})
        end

      # User returned error
      {:error, reason} ->
        error({:user_function_error, reason})

      # Raw list return (without {:ok, _} wrapper)
      result when is_list(result) ->
        if valid_kv_list?(result) do
          pure(%{task | normalized_result: result})
        else
          error({:invalid_result_format, "Expected list of {key, value} tuples"})
        end

      # Any other return type
      other ->
        error({:unexpected_return_type, other})
    end
  end

  defp mark_task_completed(%{normalized_result: result} = task) do
    completed_task = ComputeTask.mark_completed(task, result)
    pure(completed_task)
  end

  # Validation helpers
  defp valid_kv_list?(list) when is_list(list) do
    Enum.all?(list, &valid_kv_tuple?/1)
  end

  defp valid_kv_list?(_), do: false

  defp valid_kv_tuple?({key, _value}) when not is_nil(key), do: true
  defp valid_kv_tuple?(_), do: false

  # Storage functions
  def fetch_all_value_of_keys(input) do
    storage_to_keys_map = Enum.reduce(input, %{}, fn {key, storage_pids}, acc ->
      Enum.reduce(storage_pids, acc, fn storage_pid, inner_acc ->
        Map.update(inner_acc, storage_pid, [key], &[key | &1])
      end)
    end)

    storage_results =
      storage_to_keys_map
      |> Task.async_stream(fn {storage_pid, keys} ->
          case GenServer.call(storage_pid, {:get_values_of_keys, keys}) do
            {:ok, values_map} -> {:ok, storage_pid, values_map}
            {:error, reason} -> {:error, storage_pid, reason}
          end
        end,
        timeout: 30_000,
        max_concurrency: 100
      )
      |> Enum.reduce_while(%{}, fn
        {:ok, {:ok, storage_pid, values_map}}, acc ->
          {:cont, Map.put(acc, storage_pid, values_map)}

        {:ok, {:error, storage_pid, reason}}, _acc ->
          {:halt, {:error, "Storage #{inspect(storage_pid)} failed: #{inspect(reason)}"}}

        {:exit, reason}, _acc ->
          {:halt, {:error, "Task failed: #{inspect(reason)}"}}
      end)

    case storage_results do
      {:error, reason} -> {:error, reason}
      storage_results_map ->
        result_map = Enum.reduce(input, %{}, fn {key, storage_pids}, acc ->
          values = Enum.flat_map(storage_pids, fn storage_pid ->
            Map.get(storage_results_map, storage_pid, %{})
            |> Map.get(key, [])
          end)
          Map.put(acc, key, values)
        end)
        {:ok, result_map}
    end
  end

end
