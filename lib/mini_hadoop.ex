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

  def test_submit_job do
    job = %{
      input: ["test_input"],
      output: "test_output",
      map_function: fn x -> {x, x * 2} end,
      reduce_function: fn {x, y} -> {x, Enum.sum(y)} end,
      num_reducers: 2,
    }
    ComputeOperation.submit_job(job)
  end


  @doc """
  Executes a Map function against a stored block.

  Sementara berdasar blok id
  """
  def run_map(block_id, map_module, context \\ %{}) when is_binary(block_id) do
    with {:ok, worker_info} <- MiniHadoop.Master.MasterNode.lookup_block_owner(block_id),
         {:ok, payload} <-
           GenServer.call(worker_info.pid, {:run_map, block_id, map_module, context}) do
      {:ok, payload}
    else
      {:error, reason} -> {:error, reason}
    end
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
      started_at: app_start_time
    }

  end
end
