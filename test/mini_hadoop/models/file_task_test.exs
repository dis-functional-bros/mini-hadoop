defmodule MiniHadoop.Models.FileTaskTest do
  use ExUnit.Case

  alias MiniHadoop.Models.FileTask

  describe "new/1" do
    test "creates new file task with defaults" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      assert task.id == "store_1"
      assert task.type == :store
      assert task.filename == "test.txt"
      assert task.status == :pending
      assert task.progress == 0
      assert task.blocks_processed == 0
      assert task.total_blocks == 0
    end
  end

  describe "update_progress/4" do
    test "calculates progress percentage correctly" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      updated = FileTask.update_progress(task, 5, 10, "Processing blocks")
      assert updated.progress == 50.0
      assert updated.blocks_processed == 5
      assert updated.total_blocks == 10
      assert updated.message == "Processing blocks"
    end

    test "handles zero total blocks" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      updated = FileTask.update_progress(task, 0, 0)
      assert updated.progress == 0
    end

    test "handles 100% completion" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      updated = FileTask.update_progress(task, 10, 10)
      assert updated.progress == 100.0
    end

    test "rounds progress to one decimal place" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      updated = FileTask.update_progress(task, 1, 3)
      assert updated.progress == 33.3
    end
  end

  describe "mark_started/2" do
    test "sets status to started with timestamp" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      started = FileTask.mark_started(task, "Beginning store operation")
      assert started.status == :started
      assert started.started_at != nil
      assert started.message == "Beginning store operation"
    end

    test "uses default message when none provided" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      started = FileTask.mark_started(task)
      assert started.status == :started
      assert is_binary(started.message)
    end
  end

  describe "mark_running/2" do
    test "sets status to running" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })

      running = FileTask.mark_running(task, "Storing blocks")
      assert running.status == :running
      assert running.message == "Storing blocks"
    end
  end

  describe "mark_completed/2" do
    test "sets status to completed with timestamp" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })
      |> FileTask.mark_started()

      completed = FileTask.mark_completed(task, "Operation successful")
      assert completed.status == :completed
      assert completed.completed_at != nil
      assert completed.message == "Operation successful"
    end

    test "calculates total_time in seconds" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })
      |> FileTask.mark_started()

      Process.sleep(100)

      completed = FileTask.mark_completed(task)
      assert is_integer(completed.total_time) or completed.total_time == 0
    end
  end

  describe "mark_failed/3" do
    test "sets status to failed with error reason" do
      task = FileTask.new(%{
        id: "store_1",
        type: :store,
        filename: "test.txt"
      })
      |> FileTask.mark_started()

      failed = FileTask.mark_failed(task, :file_not_found, "File does not exist")
      assert failed.status == :failed
      assert failed.error_reason == :file_not_found
      assert failed.message == "File does not exist"
    end
  end
end
