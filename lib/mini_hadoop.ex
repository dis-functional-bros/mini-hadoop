defmodule MiniHadoop do
  alias MiniHadoop.Master.FileOperation
  alias MiniHadoop.Master.ComputeOperation
  alias MiniHadoop.Models.JobSpec

  def store_file(filename, file_path) when is_binary(filename) and is_binary(file_path) do
    FileOperation.store_file(filename, file_path)
  end

  def retrieve_file(filename) when is_binary(filename) do
    FileOperation.retrieve_file(filename)
  end

  def delete_file(filename) when is_binary(filename) do
    FileOperation.delete_file(filename)
  end

  def submit_job(job) when is_struct(job, JobSpec) do
    ComputeOperation.submit_job(job)
  end

  def word_count_submit_job do
    {:ok, job_spec} =
      JobSpec.create(
        job_name: "word_count_analysis",
        input_files: ["doc.txt"],
        map_function: &MiniHadoop.Examples.WordCount.word_count_mapper/2,
        reduce_function: &MiniHadoop.Examples.WordCount.word_count_reducer/2
      )

    ComputeOperation.submit_job(job_spec)
  end

  def page_rank_first_iter_submit_job do
    # PageRank First Iteration
    {:ok, init_job_spec} =
      JobSpec.create(
        job_name: "pagerank_first",
        input_files: ["adjecency.tsv"],
        map_function: &MiniHadoop.Examples.PageRank.pagerank_mapper/2,
        reduce_function: &MiniHadoop.Examples.PageRank.pagerank_reducer/2,
        map_context: %{
          damping_factor: 0.85,
          # Modify according to the total number of pages
          total_pages: 41332
        },
        sort_result_opt: {:value, :desc}
      )

    ComputeOperation.submit_job(init_job_spec)
  end

  def page_rank_second_iter_submit_job do
    shared_dir = Application.get_env(:mini_hadoop, :shared_dir)
    # PageRank Second Iteration (with Page Rank File)
    {:ok, init_job_spec} =
      JobSpec.create(
        job_name: "pagerank_second_iter",
        input_files: ["adjecency.tsv"],
        map_function: &MiniHadoop.Examples.PageRank.pagerank_mapper/2,
        reduce_function: &MiniHadoop.Examples.PageRank.pagerank_reducer/2,
        map_context: %{
          # path to file in shared directory
          pagerank_file: "#{shared_dir}/page_rank_iter_1.json",
          damping_factor: 0.85,
          # Modify according to the total number of pages
          total_pages: 41332
        },
        sort_result_opt: {:value, :desc}
      )

    ComputeOperation.submit_job(init_job_spec)
  end

  def file_op_info(task_id) when is_binary(task_id) do
    FileOperation.get_operation_info(task_id)
  end

  def job_info(job_id) when is_binary(job_id) do
    ComputeOperation.get_job_info(job_id)
  end

  def cluster_info do
    _datanodes = GenServer.call(MiniHadoop.Master.MasterNode, :list_worker)
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
      started_at: app_start_time
    }
  end
end
