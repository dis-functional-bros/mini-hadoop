# lib/mini_hadoop/models/job.ex
defmodule MiniHadoop.Models.JobSpec do
  @moduledoc """
  Represents a complete MapReduce job.
  """


  @type key :: any()
  @type value :: any()
  @type task_status :: :pending | :running | :completed | :failed

  defstruct [
    :id,
    :job_name,
    :input_files,
    :output_dir,
    :map_tasks,
    :reduce_tasks,
    :map_module,
    :reduce_module,
    :status,
    :created_at,
    :completed_at
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          job_name: String.t(),
          input_files: [String.t()],
          output_dir: String.t(),
          map_tasks: [ComputeTask.t()],
          reduce_tasks: [ComputeTask.t()],
          map_module: module(),
          reduce_module: module(),
          status: task_status(),
          created_at: DateTime.t(),
          completed_at: DateTime.t() | nil
        }

  @spec new(JobSpec.t()) :: {:ok, t()} | {:error, String.t()}
  def new(attrs) do
    attrs_map = Map.new(attrs)

    with {:ok, job_id} <- generate_job_id(),
         {:ok, _} <- validate_modules(attrs_map) do

      defaults = %{
        id: job_id,
        status: :pending,
        created_at: DateTime.utc_now(),
        completed_at: nil,
        map_tasks: [],
        reduce_tasks: []
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

  defp validate_modules(attrs) do
    map_module = Map.get(attrs, :map_module)
    reduce_module = Map.get(attrs, :reduce_module)

    cond do
      is_nil(map_module) and is_nil(reduce_module) ->
        {:error, "Missing both map_module and reduce_module"}

      is_nil(map_module) ->
        {:error, "Missing map_module"}

      is_nil(reduce_module) ->
        {:error, "Missing reduce_module"}

      not is_atom(map_module) ->
        {:error, "map_module must be a module"}

      not is_atom(reduce_module) ->
        {:error, "reduce_module must be a module"}

      not Code.ensure_loaded?(map_module) ->
        {:error, "map_module #{inspect(map_module)} is not a loaded module"}

      not Code.ensure_loaded?(reduce_module) ->
        {:error, "reduce_module #{inspect(reduce_module)} is not a loaded module"}

      # Just check if the required functions exist
      not function_exported?(map_module, :map, 2) ->
        {:error, "map_module #{inspect(map_module)} must export map/2 function"}

      not function_exported?(reduce_module, :reduce, 2) ->
        {:error, "reduce_module #{inspect(reduce_module)} must export reduce/2 function"}

      true ->
        {:ok, :modules_valid}
    end
  end
end
