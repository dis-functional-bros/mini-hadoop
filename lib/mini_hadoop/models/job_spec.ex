# lib/mini_hadoop/models/job.ex
defmodule MiniHadoop.Models.JobSpec do
  @moduledoc """
  Represents a complete MapReduce job.
  """

  @type key :: any()
  @type value :: any()
  @type task_status :: :pending | :running | :completed | :failed
  @type map_function :: (any() -> [{key(), value()}])
  @type reduce_function :: ({key(), [value()]} -> [{key(), value()}])

  defstruct [
    :job_id,
    :job_name,
    :input_files,
    :output_dir,
    :map_tasks,
    :reduce_tasks,
    :map_function,
    :reduce_function,
    :num_reducers,
    :status,
    :created_at,
    :completed_at
  ]

  @type t :: %__MODULE__{
          job_id: String.t(),
          job_name: String.t(),
          input_files: [String.t()],
          output_dir: String.t(),
          map_tasks: [ComputeTask.t()],
          reduce_tasks: [ComputeTask.t()],
          map_function: map_function(),
          reduce_function: reduce_function(),
          num_reducers: integer(),
          status: task_status(),
          created_at: DateTime.t(),
          completed_at: DateTime.t() | nil
        }

  @spec new(JobSpec.t()) :: {:ok, t()} | {:error, String.t()}
  def new(attrs) do
    attrs_map = Map.new(attrs)

    with {:ok, job_id} <- generate_job_id(),
         {:ok, _} <- validate_functions(attrs_map) do

      defaults = %{
        job_id: job_id,
        status: :pending,
        created_at: DateTime.utc_now(),
        completed_at: nil,
        map_tasks: [],
        reduce_tasks: [],
        num_reducers: 1
      }

      {:ok, struct(__MODULE__, Map.merge(defaults, attrs_map))}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def mark_started(job) do
    %{job | status: :running}
  end

  def mark_completed(job) do
    %{job |
      status: :completed,
      completed_at: DateTime.utc_now()
    }
  end

  defp generate_job_id do
    {:ok, "job_#{generate_id()}"}
  end

  defp generate_id do
    :crypto.strong_rand_bytes(2) |> Base.encode16(case: :lower)
  end

  defp validate_functions(attrs) do
    map_func = Map.get(attrs, :map_function)
    reduce_func = Map.get(attrs, :reduce_function)

    cond do
      is_nil(map_func) and is_nil(reduce_func) ->
        {:error, "Missing both map_function and reduce_function"}

      is_nil(map_func) ->
        {:error, "Missing map_function"}

      is_nil(reduce_func) ->
        {:error, "Missing reduce_function"}

      not is_function(map_func) ->
        {:error, "map_function must be a function"}

      not is_function(reduce_func) ->
        {:error, "reduce_function must be a function"}

      :erlang.fun_info(map_func, :arity) != {:arity, 1} ->
        {:error, "map_function must accept exactly 1 argument"}

      :erlang.fun_info(reduce_func, :arity) != {:arity, 1} ->
        {:error, "reduce_function must accept exactly 1 argument"}

      true ->
        {:ok, :functions_valid}
    end
  end
end
