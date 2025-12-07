defmodule MiniHadoop.ComputeTask.RunnerAndStorageSupervisor do
  @moduledoc """
  Interface for dynamically starting and managing task processes on worker nodes.
  """
  alias MiniHadoop.ComputeTask.TaskRunner
  alias MiniHadoop.ComputeTask.TaskResultStorage

  # Public API
  @spec start_task_runner(String.t(), pid(), pid()) :: {:ok, pid()} | {:error, term()}
  def start_task_runner(job_id, job_pid,storage_pid) do
    child_spec = %{
      id: {TaskRunner, job_id},
      start: {TaskRunner, :start_link, [job_id, job_pid, storage_pid]},
      restart: :transient
    }
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @spec start_temp_storage(String.t()) :: {:ok, pid()} | {:error, term()}
  def start_temp_storage(job_id) do
    child_spec = %{
      id: {TaskResultStorage, job_id},
      start: {TaskResultStorage, :start_link, [job_id]},
      restart: :transient
    }
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @spec stop_task_runner(pid()) :: :ok | {:error, :not_found}
  def stop_task_runner(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @spec stop_temp_storage(pid()) :: :ok | {:error, :not_found}
  def stop_temp_storage(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @spec stop_by_job_id(String.t()) :: :ok
  def stop_by_job_id(job_id) do
    list_tasks()
    |> Enum.each(fn
      {{TaskRunner, ^job_id}, pid, _type, _modules} -> stop_task_runner(pid)
      {{TaskResultStorage, ^job_id}, pid, _type, _modules} -> stop_temp_storage(pid)
      _ -> :ok
    end)
  end

  @spec list_tasks() :: [{term(), pid(), :worker, [module()]}]
  def list_tasks do
    DynamicSupervisor.which_children(__MODULE__)
  end

  @spec count_tasks() :: %{
    active: non_neg_integer(),
    specs: non_neg_integer(),
    supervisors: non_neg_integer(),
    workers: non_neg_integer()
  }
  def count_tasks do
    DynamicSupervisor.count_children(__MODULE__)
  end

  @spec find_task_runner_pid(String.t()) :: pid() | nil
  def find_task_runner_pid(job_id) do
    list_tasks()
    |> Enum.find_value(fn
      {{TaskRunner, ^job_id}, pid, _type, _modules} -> pid
      _ -> nil
    end)
  end

  @spec find_storage_pid(String.t()) :: pid() | nil
  def find_storage_pid(job_id) do
    list_tasks()
    |> Enum.find_value(fn
      {{TaskResultStorage, ^job_id}, pid, _type, _modules} -> pid
      _ -> nil
    end)
  end
end
