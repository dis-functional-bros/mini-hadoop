# lib/mini_hadoop/job/job_supervisor.ex
defmodule MiniHadoop.Job.JobSupervisor do
  @moduledoc """
  Interface for dynamically starting and managing job processes.
  """
  alias MiniHadoop.Models.JobSpec

  # Public API
  @spec start_job(JobSpec.t(), [pid()]) ::
        {:ok, pid()} | {:error, term()}
  def start_job(job, workers) do
    child_spec = %{
      id: {MiniHadoop.Job.JobRunner, job.id},
      start: {MiniHadoop.Job.JobRunner, :start_link, [job, workers]},
      restart: :transient # restart if abnormal termination
    }
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @spec stop_job(pid()) :: :ok | {:error, :not_found}
  def stop_job(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @spec list_jobs() :: [{:undefined, pid(), :worker, [module()]} | term()]
  def list_jobs do
    DynamicSupervisor.which_children(__MODULE__)
  end

  @spec count_jobs() :: %{
    active: non_neg_integer(),
    specs: non_neg_integer(),
    supervisors: non_neg_integer(),
    workers: non_neg_integer()
  }
  def count_jobs do
    DynamicSupervisor.count_children(__MODULE__)
  end

  @spec find_job_pid(String.t()) :: pid() | nil
  def find_job_pid(_job_id) do
    list_jobs()
    |> Enum.find_value(fn
      {_id, pid, _type, _modules} ->
        # You might want to check if this pid belongs to the job_id
        # This requires the JobProcessor to expose job information
        pid
      _ -> nil
    end)
  end
end
