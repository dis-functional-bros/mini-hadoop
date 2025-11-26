defmodule MiniHadoop.Worker.TempStorage do
  use GenServer

  defstruct [:data_dir, :job_buckets]

  def init(opts) do
    data_dir = opts[:data_dir] || "/tmp/mini_hadoop"
    File.mkdir_p!(data_dir)

    {:ok, %__MODULE__{data_dir: data_dir, job_buckets: %{}}}
  end

  def handle_call({:store_map_result, job_id, _task_id, kvs}, _from, state) do
    # Store intermediate result in memory

    {:reply, :ok, state}
  end

  def handle_call({:get_intermediate_data, job_id}, _from, state) do
    # Get all combined data for a job (for reduce phase)
    # expected output : {:ok, ["key1", "key2", "key3"]}

    {:reply,:ok ,state}
  end

  def handle_cast({:clean_data, job_id}, state) do
    # Clean in-memory data for after a job is completed
    {:noreply, state}
  end

end
