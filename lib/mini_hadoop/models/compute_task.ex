# lib/mini_hadoop/models/compute_task.ex
defmodule MiniHadoop.Models.ComputeTask do
  @moduledoc """
  Represents a single computation unit (map or reduce task) for MapReduce.
  Named ComputeTask to avoid confusion with Elixir's Task module.
  """
  alias MiniHadoop.Models.Types

  defstruct [
    :id,
    :job_id,
    :type,
    :status,
    :input_data,  # For map: {block_id, [worker_pid]}, for reduce: [{key(), [pid()]}]
    :output_data,
    :data,        # holds fetched data
    :function,
    :context,
    :started_at,
    :completed_at,
    :error,
    :raw_result,  #  holds user function raw output
    :normalized_result #  holds validated result
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          job_id: String.t(),
          type: Types.task_type(),
          status: Types.status(),
          input_data: any(),
          output_data: Types.result() | nil,
          data: any() | nil,
          function: Types.map_function() | Types.reduce_function(),
          context: map(),
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          error: term() | nil,
          raw_result: term() | nil,
          normalized_result: Types.result() | nil
        }

  @spec new(map()) :: t()
  def new(attrs) do
    defaults = %{
      status: :pending,
      started_at: nil,
      completed_at: nil,
      data: nil,
      raw_result: nil,
      normalized_result: nil,
      error: nil
    }

    struct(__MODULE__, Map.merge(defaults, Map.new(attrs)))
  end

  # Fixed spec - input_data should be tuple, not map
  @spec new_map(String.t(), {String.t(), [pid()]}, Types.map_function(), map()) :: t()
  def new_map(job_id, block_info, map_function, context) do
    task_id = "map_#{job_id}_#{generate_id()}"

    new(%{
      id: task_id,
      job_id: job_id,
      type: :map,
      input_data: block_info,  # This is a tuple {block_id, [storage_pids]}
      function: map_function,
      context: context
    })
  end

  @spec new_reduce(String.t(), [{String.t(), String.t(),[pid()]}], Types.reduce_function(), map()) :: t()
  def new_reduce(job_id, keys_range_and_storage_pids, reduce_function, context) do
    task_id = "red_#{job_id}_#{generate_id()}"

    new(%{
      id: task_id,
      job_id: job_id,
      type: :reduce,
      input_data: keys_range_and_storage_pids,  # This is a list of {key, [storage_pids]}
      function: reduce_function,
      context: context
    })
  end

  def mark_started(task) do
    %{task | status: :running, started_at: DateTime.utc_now()}
  end

  def mark_completed(task, output_data) do
    %{task |
      status: :completed,
      output_data: output_data,
      completed_at: DateTime.utc_now()
    }
  end

  def mark_failed(task, reason) do
    %{task |
      status: :failed,
      completed_at: DateTime.utc_now(),
      error: reason
    }
  end

  defp generate_id do
    :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
  end
end
