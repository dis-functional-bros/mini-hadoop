# lib/mini_hadoop/job/job_runner.ex
defmodule MiniHadoop.Job.JobRunner do
  use GenServer
  require Logger

  defstruct [
    :job,
    :map_tasks,
    :reduce_tasks,
    :map_results,
    :status,
    :started_at
  ]

  # Start via DynamicSupervisor
  def start_link(job) do
    GenServer.start_link(__MODULE__, job)
  end

  @impl true
  def init(job) do
    state = %__MODULE__{
      job: job,
      status: :starting,
      started_at: DateTime.utc_now()
    }

    # Notify ComputeOperation
    GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_started, job.job_id})

    # Start processing asynchronously
    Process.send(self(), :start_processing, [])

    {:ok, state}
  end

  def handle_info(:start_processing, state) do
    Logger.info("Job #{state.job.job_id}: Starting processing")

    try do
      # Execute map phase
      state = exec_map_phase(state)

      # Execute reduce phase
      exec_reduce_phase(state)

    rescue
      error ->
        Logger.error("Job #{state.job.job_id} failed: #{inspect(error)}")
        GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_failed, state.job.job_id, error})
        {:stop, :normal, state}
    end
  end

  # Function for executing map phase
  def exec_map_phase(state) do
    GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_status, state.job.job_id, :map_phase})
    # Job spec will have list of input file, fetch their block locations from Master.MasterNode
    # block_informations = MiniHadoop.Master.MasterNode.fetch_blocks_by_filenames(state.job.input_files)

    # Generate map tasks
    # map_tasks = generate_map_compute_tasks(block_informations)


    # Simulation of map task execution
    total_map_tasks = 5
    Logger.info("Starting #{total_map_tasks} map tasks")

    Enum.each(1..total_map_tasks, fn task_num ->
      Process.sleep(2000)  # 2 seconds per map task
      Logger.info("Map task #{task_num}/#{total_map_tasks} completed")

      # Update progress - completed count and total
      GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_progress, state.job.job_id, :map, task_num, total_map_tasks})
    end)

    # All map tasks completed
    Logger.info("All #{total_map_tasks} map tasks completed")

    # Return updated state to continue to reduce phase
    %{state | status: :map_completed}
  end

  # Function for executing reduce phase
  def exec_reduce_phase(state) do
    GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_status, state.job.job_id, :reduce_phase})

    # Simulate reduce tasks with progress reporting
    total_reduce_tasks = 3
    Logger.info("Starting #{total_reduce_tasks} reduce tasks")

    Enum.each(1..total_reduce_tasks, fn task_num ->
      Process.sleep(3000)  # 3 seconds per reduce task
      Logger.info("Reduce task #{task_num}/#{total_reduce_tasks} completed")

      # Update progress
      GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_progress, state.job.job_id, :reduce, task_num, total_reduce_tasks})
    end)

    # All reduce tasks completed
    Logger.info("All #{total_reduce_tasks} reduce tasks completed")

    # Generate final results
    results = %{
      total_map_tasks: 5,
      total_reduce_tasks: total_reduce_tasks,
      output_files: [
        "/output/#{state.job.job_id}/part-00000",
        "/output/#{state.job.job_id}/part-00001"
      ],
      summary: %{
        total_records_processed: 5000,  # Simulated data
        unique_keys: 150,  # Simulated data
        data_size_bytes: 250000  # Simulated data
      }
    }

    # Notify completion
    GenServer.cast(MiniHadoop.Master.ComputeOperation, {:job_completed, state.job.job_id, results})

    Logger.info("Job #{state.job.job_id}: Completed, shutting down")
    {:stop, :normal, state}
  end

  @type block_information :: %{String.t() => [pid()]}
  @type task_map :: %{String.t() => MiniHadoop.Models.ComputeTask.t()}

  #TODO
  @spec generate_map_compute_tasks(block_information()) :: task_map()

  defp generate_map_compute_tasks(block_informations) do
    # Input: %{block_id => [worker_ids]}
    # Output: %{task_id => ComputeTask.t()}

    # Implementation here

  end


end
