defmodule MiniHadoop.Common.FileTask do
  @moduledoc """
  Simplified file task tracking with block-based progress.
  """

  defstruct [
    :id,
    :type,        # :store, :read, :delete
    :filename,
    :file_path,   # For store operations
    :status,      # :pending, :running, :completed, :failed
    :progress,    # 0-100
    :result,
    :error_reason,
    :started_at,
    :completed_at,
    :total_time,  # in milliseconds
    :created_by,  # PID or node
    :blocks_processed,
    :total_blocks,
    :message      # Current status message
  ]

  def new(attrs) do
    defaults = %{
      status: :pending,
      progress: 0,
      blocks_processed: 0,
      total_blocks: 0,
      started_at: nil,
      completed_at: nil,
      total_time: nil,
      created_by: self(),
      message: "Pending"
    }

    struct(__MODULE__, Map.merge(defaults, attrs))
  end

  def update_progress(task, blocks_processed, total_blocks, message \\ nil) do
    progress = if total_blocks > 0 do
      Float.round(blocks_processed / total_blocks * 100, 1)
    else
      0
    end

    %{task |
      progress: progress,
      blocks_processed: blocks_processed,
      total_blocks: total_blocks,
      message: message || task.message
    }
  end

  def mark_running(task, message \\ "Starting operation") do
    %{task |
      status: :running,
      started_at: DateTime.utc_now(),
      message: message
    }
  end

  def mark_completed(task, result, message \\ "Completed") do
    completed_at = DateTime.utc_now()
    total_time = calculate_total_time(task.started_at, completed_at)

    %{task |
      status: :completed,
      progress: 100,
      result: result,
      completed_at: completed_at,
      total_time: total_time,
      message: message
    }
  end

  def mark_failed(task, reason, message \\ "Operation failed") do
    completed_at = DateTime.utc_now()
    total_time = calculate_total_time(task.started_at, completed_at)

    %{task |
      status: :failed,
      error_reason: reason,
      completed_at: completed_at,
      total_time: total_time,
      message: message
    }
  end

  defp calculate_total_time(nil, _completed_at), do: nil
  defp calculate_total_time(started_at, completed_at) do
    DateTime.diff(completed_at, started_at, :millisecond)
  end
end
