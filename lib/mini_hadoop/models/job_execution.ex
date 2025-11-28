# lib/mini_hadoop/models/job_execution.ex
defmodule MiniHadoop.Models.JobExecution do
  @moduledoc """
  Represents the runtime execution state of a MapReduce job.
  """

  @type task_status :: :pending | :running | :completed | :failed
  @type progress :: %{
    map: {completed :: non_neg_integer(), total :: non_neg_integer()},
    reduce: {completed :: non_neg_integer(), total :: non_neg_integer()}
  }

  defstruct [
    :job_id,           # Reference to JobSpec in job_specs map
    :pid,              # Process PID running this job
    :status,           # task_status()
    :map_tasks,        # [MiniHadoop.Models.ComputeTask.t()]
    :reduce_tasks,     # [MiniHadoop.Models.ComputeTask.t()]
    :created_at,
    :started_at,
    :completed_at,
    :elapsed_time_ms,
    :progress,         # progress()
    :results,          # Final job results
    :error             # Error info if job failed
  ]

  @type t :: %__MODULE__{
          job_id: String.t(),
          pid: pid() | nil,
          status: task_status(),
          map_tasks: [MiniHadoop.Models.ComputeTask.t()],
          reduce_tasks: [MiniHadoop.Models.ComputeTask.t()],
          created_at: DateTime.t(),
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          elapsed_time_ms: non_neg_integer() | nil,
          progress: progress(),
          results: any() | nil,
          error: any() | nil
        }

  @spec new(String.t()) :: t()  # Changed: now only needs job_id
  def new(job_id) do
    %__MODULE__{
      job_id: job_id,
      status: :pending,
      created_at: DateTime.utc_now(),
      progress: %{map: {nil, nil}, reduce: {nil, nil}},
      map_tasks: [],
      reduce_tasks: []
    }
  end

  @spec mark_started(t(), pid()) :: t()
  def mark_started(job_execution, job_pid) do
    %{job_execution |
      pid: job_pid,
      status: :running,
      started_at: DateTime.utc_now()
    }
  end

  @spec mark_completed(t(), any()) :: t()
  def mark_completed(job_execution, results) do
    %{job_execution |
      status: :completed,
      completed_at: DateTime.utc_now(),
      elapsed_time_ms: DateTime.diff(DateTime.utc_now(), job_execution.started_at, :millisecond),
      results: results
    }
  end

  @spec mark_failed(t(), any()) :: t()
  def mark_failed(job_execution, error) do
    %{job_execution |
      status: :failed,
      completed_at: DateTime.utc_now(),
      error: error
    }
  end

  @spec update_progress(t(), :map | :reduce, completed :: non_neg_integer(), total :: non_neg_integer()) :: t()
  def update_progress(job_execution, phase, completed, total) do
    progress = Map.put(job_execution.progress, phase, {completed, total})
    %{job_execution | progress: progress}
  end

  @spec update_tasks(t(), :map | :reduce, [MiniHadoop.Models.ComputeTask.t()]) :: t()
  def update_tasks(job_execution, :map, map_tasks) do
    total = length(map_tasks)
    progress = Map.put(job_execution.progress, :map, {0, total})
    %{job_execution | map_tasks: map_tasks, progress: progress}
  end

  def update_tasks(job_execution, :reduce, reduce_tasks) do
    total = length(reduce_tasks)
    progress = Map.put(job_execution.progress, :reduce, {0, total})
    %{job_execution | reduce_tasks: reduce_tasks, progress: progress}
  end

  @spec get_progress_percentage(t()) :: %{map: float(), reduce: float()}
  def get_progress_percentage(job_execution) do
    %{map: {map_completed, map_total}, reduce: {reduce_completed, reduce_total}} = job_execution.progress

    %{
      map: calculate_percentage(map_completed, map_total),
      reduce: calculate_percentage(reduce_completed, reduce_total)
    }
  end

  defp calculate_percentage(completed, total) when total > 0 do
    Float.round(completed / total * 100, 1)
  end

  defp calculate_percentage(_, _), do: 0.0
end
