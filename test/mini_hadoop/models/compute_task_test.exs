defmodule MiniHadoop.Models.ComputeTaskTest do
  use ExUnit.Case

  alias MiniHadoop.Models.ComputeTask
  alias MiniHadoop.Models.Types

  describe "new_map/4" do
    test "creates new map task with correct structure" do
      block_info = {"block_1", [self()]}
      map_function = fn _data, _context -> [] end
      context = %{param: "value"}

      task = ComputeTask.new_map("job_1", block_info, map_function, context)

      assert task.type == :map
      assert task.job_id == "job_1"
      assert task.input_data == block_info
      assert task.function == map_function
      assert task.context == context
      assert task.status == :pending
      assert is_binary(task.id)
      assert String.starts_with?(task.id, "map_")
    end
  end

  describe "new_reduce/4" do
    test "creates new reduce task with correct structure" do
      keys_range_and_storage = [{"key1", [self()]}, {"key2", [self()]}]
      reduce_function = fn _data, _context -> {:ok, {}} end
      context = %{param: "value"}

      task = ComputeTask.new_reduce("job_1", keys_range_and_storage, reduce_function, context)

      assert task.type == :reduce
      assert task.job_id == "job_1"
      assert task.input_data == keys_range_and_storage
      assert task.function == reduce_function
      assert task.context == context
      assert task.status == :pending
      assert is_binary(task.id)
      assert String.starts_with?(task.id, "red_")
    end
  end

  describe "mark_started/1" do
    test "updates task status to running with timestamp" do
      task = ComputeTask.new_map("job_1", {"block_1", []}, fn _, _ -> [] end, %{})
      started_task = ComputeTask.mark_started(task)

      assert started_task.status == :running
      assert started_task.started_at != nil
      assert is_struct(started_task.started_at, DateTime)
    end
  end

  describe "mark_completed/2" do
    test "updates task status to completed with output data" do
      task = ComputeTask.new_map("job_1", {"block_1", []}, fn _, _ -> [] end, %{})
      started_task = ComputeTask.mark_started(task)

      output_data = [{"key", "value"}]
      completed_task = ComputeTask.mark_completed(started_task, output_data)

      assert completed_task.status == :completed
      assert completed_task.completed_at != nil
      assert completed_task.raw_result == output_data
    end
  end

  describe "mark_failed/2" do
    test "updates task status to failed with error reason" do
      task = ComputeTask.new_map("job_1", {"block_1", []}, fn _, _ -> [] end, %{})
      started_task = ComputeTask.mark_started(task)

      failed_task = ComputeTask.mark_failed(started_task, :timeout)

      assert failed_task.status == :failed
      assert failed_task.error == :timeout
      assert failed_task.completed_at != nil
    end
  end

  describe "task timing" do
    test "calculates time difference correctly" do
      task = ComputeTask.new_map("job_1", {"block_1", []}, fn _, _ -> [] end, %{})
      started_task = ComputeTask.mark_started(task)

      Process.sleep(100)

      completed_task = ComputeTask.mark_completed(started_task, [])

      # Should have some time elapsed
      assert completed_task.completed_at != nil
      assert DateTime.compare(completed_task.completed_at, completed_task.started_at) in [:gt, :eq]
    end
  end
end
