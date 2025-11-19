defmodule MiniHadoop.Common.FileTask do
  @moduledoc """
  Simplified file task tracking with block-based progress.
  """

  defstruct [
    :id,
    # :store, :read, :delete
    :type,
    :filename,
    :file_path,
    # :pending, :running, :completed, :failed
    :status,
    # 0-100
    :progress,
    :error_reason,
    :started_at,
    :completed_at,
    # in milliseconds
    :total_time,
    # PID or node
    :created_by,
    :blocks_processed,
    :total_blocks,
    # Current status message
    :message
  ]

  @default_attrs %{
    status: :pending,
    progress: 0,
    blocks_processed: 0,
    total_blocks: 0,
    started_at: nil,
    completed_at: nil,
    total_time: nil,
    created_by: nil,
    message: "Pending"
  }

  def new(attrs) do
    struct(__MODULE__, Map.merge(@default_attrs, Map.put(attrs, :created_by, self())))
  end

  # --------------------
  # Progress updater
  def update_progress(task, blocks_processed, total_blocks, message \\ nil) do
    progress =
      if total_blocks > 0 do
        Float.round(blocks_processed / total_blocks * 100, 1)
      else
        0
      end

    %{
      task
      | progress: progress,
        blocks_processed: blocks_processed,
        total_blocks: total_blocks,
        message: message || task.message
    }
  end

  # --------------------
  # Generic status updater
  defp mark_status(task, status, message, reason \\ nil) do
    now = DateTime.utc_now()

    {started_at, completed_at, total_time} =
      case status do
        :running ->
          # hanya set started_at
          {now, task.completed_at, task.total_time}

        :completed ->
          # set completed_at + hitung total_time
          {task.started_at, now, calculate_total_time(task.started_at, now)}

        :failed ->
          # set completed_at + hitung total_time
          {task.started_at, now, calculate_total_time(task.started_at, now)}

        _ ->
          {task.started_at, task.completed_at, task.total_time}
      end

    %{
      task
      | status: status,
        message: message || task.message,
        started_at: started_at,
        completed_at: completed_at,
        total_time: total_time,
        error_reason: reason
    }
  end

  # --------------------
  # Public helpers
  def mark_running(task, message \\ "Starting operation"),
    do: mark_status(%{task | started_at: DateTime.utc_now()}, :running, message)

  def mark_completed(task, message \\ "Completed"),
    do: mark_status(task, :completed, message)

  def mark_failed(task, reason, message \\ "Operation failed"),
    do: mark_status(task, :failed, message, reason)

  defp calculate_total_time(started_at, completed_at),
    do: DateTime.diff(completed_at, started_at, :millisecond)
end
