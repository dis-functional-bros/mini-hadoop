defmodule MiniHadoop.Models.JobSpecTest do
  use ExUnit.Case, async: false

  alias MiniHadoop.Models.JobSpec
  alias MiniHadoop.Master.MasterNode

  setup_all do
    {:ok, _pid} = start_supervised(MasterNode)
    :ok
  end

  describe "create/1" do
    test "creates valid job spec with required fields" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      assert job_spec.job_name == "test_job"
      assert job_spec.input_files == ["test.txt"]
      assert is_binary(job_spec.id)
    end

    test "creates job spec with optional context parameters" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "context_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2,
        map_context: %{param1: "value1"},
        reduce_context: %{param2: "value2"}
      )

      assert job_spec.map_context == %{param1: "value1"}
      assert job_spec.reduce_context == %{param2: "value2"}
    end

    test "creates job spec with sort options" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "sorted_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2,
        sort_result_opt: {:value, :desc}
      )

      assert job_spec.sort_result_opt == {:value, :desc}
    end

    test "rejects invalid job name" do
      {:error, reason} = JobSpec.create(
        job_name: "",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      assert is_binary(reason)
      assert String.contains?(reason, "job_name")
    end

    test "rejects missing input files" do
      {:error, reason} = JobSpec.create(
        job_name: "test_job",
        input_files: [],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      assert is_binary(reason)
    end

    test "rejects missing map function" do
      {:error, reason} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        reduce_function: &dummy_reducer/2
      )

      assert is_binary(reason)
    end

    test "rejects missing reduce function" do
      {:error, reason} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2
      )

      assert is_binary(reason)
    end

    test "normalizes 1-arity map function to 2-arity" do
      one_arity_mapper = fn line ->
        line
        |> String.split()
        |> Enum.map(fn word -> {word, 1} end)
      end

      {:ok, job_spec} = JobSpec.create(
        job_name: "arity_test",
        input_files: ["test.txt"],
        map_function: one_arity_mapper,
        reduce_function: &dummy_reducer/2
      )

      # Should not raise an error
      assert job_spec.map_function != nil
    end

    test "rejects function with invalid arity" do
      invalid_function = fn _a, _b, _c -> :ok end

      {:error, reason} = JobSpec.create(
        job_name: "invalid_arity",
        input_files: ["test.txt"],
        map_function: invalid_function,
        reduce_function: &dummy_reducer/2
      )

      assert String.contains?(reason, "arity")
    end

    test "rejects invalid sort_result_opt" do
      {:error, reason} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2,
        sort_result_opt: "invalid"
      )

      assert is_binary(reason)
    end

    test "validates sort_result_opt with valid tuple" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2,
        sort_result_opt: {:key, :asc}
      )

      assert job_spec.sort_result_opt == {:key, :asc}
    end
  end

  describe "get_normalized_functions/1" do
    test "returns normalized map and reduce functions" do
      {:ok, job_spec} = JobSpec.create(
        job_name: "test_job",
        input_files: ["test.txt"],
        map_function: &dummy_mapper/2,
        reduce_function: &dummy_reducer/2
      )

      {map_fn, reduce_fn} = JobSpec.get_normalized_functions(job_spec)
      assert is_function(map_fn, 2)
      assert is_function(reduce_fn, 2)
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
