defmodule MiniHadoop do
  require Logger
  alias MiniHadoop.Master.FileOperation
  alias MiniHadoop.Master.ComputeOperation

  def store_file(filename, file_path) when is_binary(filename) and is_binary(file_path) do
    FileOperation.store_file(filename, file_path)
  end

  def retrieve_file(filename) when is_binary(filename) do
    FileOperation.retrieve_file(filename)
  end

  def delete_file(filename) when is_binary(filename) do
    FileOperation.delete_file(filename)
  end

  def submit_job(job) when is_map(job) do
    ComputeOperation.submit_job(job)
  end

  def test_submit_job do
    # test create a job spec
    {:ok, job_spec} = MiniHadoop.Models.JobSpec.create([
      job_name: "word_count_analysis",
      input_files: ["med.txt"],
      map_function: &MiniHadoop.Examples.WordCount.word_count_mapper/2,
      reduce_function: &MiniHadoop.Examples.WordCount.word_count_reducer/2
    ])

    ComputeOperation.submit_job(job_spec)
  end

  def file_op_info(task_id) when is_binary(task_id) do
    FileOperation.get_operation_info(task_id)
  end

  def cluster_info do
    datanodes = GenServer.call(MiniHadoop.Master.MasterNode, :list_worker)
    master_state = GenServer.call(MiniHadoop.Master.MasterNode, :get_state)

    # Get application start time and calculate uptime
    app_start_time = Application.get_env(:mini_hadoop, :start_time, DateTime.utc_now())
    now = DateTime.utc_now()
    uptime_seconds = DateTime.diff(now, app_start_time, :second)

    days = div(uptime_seconds, 86400)
    hours = div(rem(uptime_seconds, 86400), 3600)
    minutes = div(rem(uptime_seconds, 3600), 60)
    seconds = rem(uptime_seconds, 60)

    uptime = %{
      days: days,
      hours: hours,
      minutes: minutes,
      seconds: seconds
    }

    %{
      master_state: master_state,
      uptime: uptime,
      started_at: app_start_time,
      # datanodes: datanodes  master_state already have datanodes info
    }
  end
end
