defmodule MiniHadoop.Master.ComputeOperationTest do
  use ExUnit.Case, async: false

  alias MiniHadoop.Master.ComputeOperation
  alias MiniHadoop.Models.JobSpec
  alias MiniHadoop.Master.MasterNode

  @moduletag :integration

  setup_all do
    {:ok, _master} = start_supervised(MasterNode)
    {:ok, _compute_op} = start_supervised(ComputeOperation)
    :ok
  end

  describe "submit_job/1" do
    test "submits a valid job" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      result = ComputeOperation.submit_job(job_spec)
      assert {:ok, job_id} = result
      assert is_binary(job_id)
    end

    test "rejects job with invalid spec" do
      invalid_spec = %{}
      result = ComputeOperation.submit_job(invalid_spec)
      assert {:error, _reason} = result
    end
  end

  describe "get_job_info/1" do
    test "returns job information for submitted job" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "info_test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      {:ok, job_id} = ComputeOperation.submit_job(job_spec)
      Process.sleep(100)

      info = ComputeOperation.get_job_info(job_id)
      assert info != nil
      assert Map.has_key?(info, :job_id)
    end

    test "returns nil for non-existent job" do
      info = ComputeOperation.get_job_info("nonexistent_job")
      assert info == nil
    end
  end

  describe "list_jobs/0" do
    test "returns list of all jobs" do
      jobs = ComputeOperation.list_jobs()
      assert is_list(jobs)
    end

    test "includes submitted jobs in list" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "list_test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      {:ok, job_id} = ComputeOperation.submit_job(job_spec)
      Process.sleep(100)

      jobs = ComputeOperation.list_jobs()
      job_ids = Enum.map(jobs, & &1.id)
      assert job_id in job_ids
    end
  end

  # Helper functions
  defp dummy_mapper(line, _context) do
    line
    |> String.split()
    |> Enum.map(fn word -> {word, 1} end)
  end

  defp dummy_reducer({word, counts}, _context) do
    {word, Enum.sum(counts)}
  end
end
