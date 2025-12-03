# lib/mini_hadoop/master/compute_operation.ex
defmodule MiniHadoop.Master.ComputeOperation do
  use GenServer
  require Logger

  alias MiniHadoop.Job.JobSupervisor
  alias MiniHadoop.Models.JobSpec
  alias MiniHadoop.Models.JobExecution

  @max_concurrent_jobs Application.get_env(:mini_hadoop, :max_concurrent_jobs, 1)
  @max_queue_size_of_jobs Application.get_env(:mini_hadoop, :max_queue_size_of_jobs, 10)

  defstruct [
    # Job Specifications (immutable definitions)
    # %{job_id => JobSpec.t()}
    job_specs: %{},

    # Job Executions (runtime state)
    # %{job_id => JobExecution.t()}
    job_executions: %{},

    # Process tracking
    # %{job_id => pid()}
    job_processes: %{},

    # Worker management
    # [worker_pid]
    workers: [],

    # Job queuing system
    # Queue of job_ids waiting to run
    pending_jobs: :queue.new(),
    # %{job_id => %{started_at: DateTime.t()}}
    running_jobs: %{},

    # System metrics
    metrics: %{
      total_jobs_submitted: 0,
      total_jobs_completed: 0,
      total_jobs_failed: 0,
      active_workers: 0
    }
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do

    state = %__MODULE__{
      job_specs: %{},
      job_executions: %{},
      job_processes: %{},
      workers: [],
      pending_jobs: :queue.new(),
      running_jobs: %{},
      metrics: %{
        total_jobs_submitted: 0,
        total_jobs_completed: 0,
        total_jobs_failed: 0,
        active_workers: 0
      }
    }

    {:ok, state}
  end

  # Public API
  def submit_job(job_attrs) do
    GenServer.call(__MODULE__, {:submit_job, job_attrs})
  end

  def register_worker(worker_pid) do
    GenServer.call(__MODULE__, {:register_worker, worker_pid})
  end

  def get_workers do
    GenServer.call(__MODULE__, :get_workers)
  end

  def get_job_status(job_id) do
    GenServer.call(__MODULE__, {:get_job_status, job_id})
  end

  def list_running_jobs do
    GenServer.call(__MODULE__, :list_running_jobs)
  end

  def list_pending_jobs do
    GenServer.call(__MODULE__, :list_pending_jobs)
  end

  def get_system_status do
    GenServer.call(__MODULE__, :get_system_status)
  end

  # GenServer Handlers
  def handle_call({:register_worker, worker_pid}, _from, state) do
    {:reply, :ok, %{state | workers: [worker_pid | state.workers]}}
  end

  def handle_call(:get_workers, _from, state) do
    {:reply, state.workers, state}
  end

  def handle_call({:submit_job, job_spec}, _from, state) do

    unless is_struct(job_spec, MiniHadoop.Models.JobSpec) do
      {:reply, {:error, :invalid_job_spec}, state}
    end

    if :queue.len(state.pending_jobs) > @max_queue_size_of_jobs do
      {:reply, {:error, :queue_full}, state}
    else

      job_execution = JobExecution.new(job_spec.id)

      state = %{
        state
        | job_specs: Map.put(state.job_specs, job_spec.id, job_spec),
          job_executions: Map.put(state.job_executions, job_spec.id, job_execution),
          pending_jobs: :queue.in(job_spec.id, state.pending_jobs),
          metrics: %{
            state.metrics
            | total_jobs_submitted: state.metrics.total_jobs_submitted + 1
          }
      }

      state = start_pending_jobs(state)

      {:reply, {:ok, job_spec.id}, state}
    end
  end

  def handle_call({:get_job_status, job_id}, _from, state) do
    case state.job_executions[job_id] do
      nil ->
        {:reply, {:error, :not_found}, state}

      job_execution ->
        {:reply, {:ok, job_execution}, state}
    end
  end

  def handle_call(:list_running_jobs, _from, state) do
    running_jobs = Map.keys(state.running_jobs)
    {:reply, running_jobs, state}
  end

  def handle_call(:list_pending_jobs, _from, state) do
    pending_jobs = :queue.to_list(state.pending_jobs)
    {:reply, pending_jobs, state}
  end

  def handle_call(:get_system_status, _from, state) do
    status = %{
      running_jobs: map_size(state.running_jobs),
      pending_jobs: :queue.len(state.pending_jobs),
      max_concurrent_jobs: @max_concurrent_jobs,
      active_workers: length(state.workers),
      metrics: state.metrics
    }

    {:reply, status, state}
  end

  # Async update dari Job Runner
  def handle_cast({:job_started, job_id}, state) do

    state =
      update_in(state.job_executions[job_id], fn job_execution ->
        JobExecution.mark_started(job_execution, state.job_processes[job_id])
      end)

    {:noreply, state}
  end

  def handle_cast({:job_status, job_id, status}, state) do
    state =
      update_in(state.job_executions[job_id], fn job_execution ->
        %{job_execution | status: status}
      end)

    {:noreply, state}
  end

  def handle_cast({:job_progress, job_id, phase, completed, total}, state) do
    state =
      update_in(state.job_executions[job_id], fn job_execution ->
        JobExecution.update_progress(job_execution, phase, completed, total)
      end)

    {:noreply, state}
  end

  def handle_cast({:job_completed, job_id, results}, state) do
    state =
      update_in(state.job_executions[job_id], fn job_execution ->
        JobExecution.mark_completed(job_execution, results)
      end)

    state = %{
      state
      | running_jobs: Map.delete(state.running_jobs, job_id),
        job_processes: Map.delete(state.job_processes, job_id)
    }

    state = %{
      state
      | metrics: %{state.metrics | total_jobs_completed: state.metrics.total_jobs_completed + 1}
    }

    state = start_pending_jobs(state)

    {:noreply, state}
  end

  def handle_cast({:job_failed, job_id, reason}, state) do
    Logger.error("Job #{job_id} failed: #{inspect(reason)}")

    state =
      update_in(state.job_executions[job_id], fn job_execution ->
        JobExecution.mark_failed(job_execution, reason)
      end)

    state = %{
      state
      | running_jobs: Map.delete(state.running_jobs, job_id),
        job_processes: Map.delete(state.job_processes, job_id)
    }

    state = %{
      state
      | metrics: %{state.metrics | total_jobs_failed: state.metrics.total_jobs_failed + 1}
    }

    state = start_pending_jobs(state)

    {:noreply, state}
  end

  # Handle unexpected process termination
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do

    case Enum.find(state.job_processes, fn {_job_id, job_pid} -> job_pid == pid end) do
      {job_id, _pid} ->
        Logger.warning("Job #{job_id} process terminated unexpectedly: #{inspect(reason)}")

        state =
          update_in(state.job_executions[job_id], fn job_execution ->
            JobExecution.mark_failed(job_execution, :unexpected_termination)
          end)

        state = %{
          state
          | running_jobs: Map.delete(state.running_jobs, job_id),
            job_processes: Map.delete(state.job_processes, job_id)
        }

        state = start_pending_jobs(state)
        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  # Private functions
  defp start_pending_jobs(state) do
    if map_size(state.running_jobs) < @max_concurrent_jobs and
         not :queue.is_empty(state.pending_jobs) do
      {{:value, job_id}, pending_jobs} = :queue.out(state.pending_jobs)
      state_with_updated_queue = %{state | pending_jobs: pending_jobs}

      case start_job_process(job_id, state_with_updated_queue) do
        {:ok, new_state} ->
          start_pending_jobs(new_state)

        {:error, reason, new_state} ->
          Logger.error("Failed to start job #{job_id}:#{reason}")
          start_pending_jobs(new_state)
      end
    else
      state
    end
  end

  defp start_job_process(job_id, state) do
    job_spec = state.job_specs[job_id]

    case JobSupervisor.start_job(job_spec, state.workers) do
      {:ok, job_pid} ->
        running_jobs =
          Map.put(state.running_jobs, job_id, %{
            started_at: DateTime.utc_now()
          })

        job_processes = Map.put(state.job_processes, job_id, job_pid)

        state = %{state | running_jobs: running_jobs, job_processes: job_processes}

        {:ok, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end
end
