# lib/mini_hadoop/models/compute_task.ex
defmodule MiniHadoop.Models.ComputeTask do
  @moduledoc """
  Represents a single computation unit (map or reduce task) for MapReduce.
  Named ComputeTask to avoid confusion with Elixir's Task module.
  """

  @type task_type :: :map | :reduce
  @type task_status :: :pending | :running | :completed | :failed
  @type key :: any()
  @type value :: any()
  @type intermediate_data :: [{key(), value()}]
  @type map_function :: (any() -> [{key(), value()}])
  @type reduce_function :: ({key(), [value()]} -> [{key(), value()}])

  defstruct [
    :task_id,
    :job_id,
    :job_ref,
    :type,
    :status,
    :input_data,  # For map: {block_id=>worker_pid}, for reduce: list of intermediate key-value pairs
    :output_data,
    :function,
    :key,
    :attempt,
    :started_at,
    :completed_at
  ]

  @type t :: %__MODULE__{
          task_id: String.t(),
          job_id: String.t(),
          job_ref: pid(),
          type: task_type(),
          status: task_status(),
          input_data: any(),
          output_data: intermediate_data() | any(),
          function: map_function() | reduce_function(),
          key: key() | nil,
          attempt: integer(),
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil
        }

  @spec new(map()) :: t()
  def new(attrs) do
    defaults = %{
      status: :pending,
      attempt: 0,
      started_at: nil,
      completed_at: nil
    }

    struct(__MODULE__, Map.merge(defaults, Map.new(attrs)))
  end

  # Change spec from map to tuple
  @spec new_map(String.t(), {String.t(), [pid()]}, map_function(), pid()) :: t()
  def new_map(job_id, block_info, map_function, job_ref) do
    task_id = "map_#{job_id}_#{generate_id()}"

    new(%{
      task_id: task_id,
      job_id: job_id,
      type: :map,
      input_data: block_info,  # This will now be a tuple
      function: map_function,
      job_ref: job_ref
    })
  end

  @spec new_reduce(String.t(), key(), [{String.t(), [pid()]}], reduce_function(), pid()) :: t()
  def new_reduce(job_id, key, input_data, reduce_function, job_ref) do
    task_id = "red_#{job_id}_#{generate_id()}"

    new(%{
      task_id: task_id,
      job_id: job_id,
      job_ref: job_ref,
      type: :reduce,
      key: key,
      input_data: input_data,
      function: reduce_function
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

  def mark_failed(task) do
    %{task |
      status: :failed,
      completed_at: DateTime.utc_now(),
      attempt: task.attempt + 1
    }
  end

  defp generate_id do
    :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
  end
end
