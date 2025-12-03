defmodule MiniHadoop.Models.FileTask do
  @moduledoc """
  Simplified file task tracking with block-based progress.
  """

  defstruct [
    :id,
    :type,       # :store, :read, :delete
    :filename,
    :file_path,
    :status,        # :pending, :running, :completed, :failed
    :progress,     # 0-100
    :error_reason,
    :started_at,
    :completed_at,
    :total_time,  # in milliseconds
    :created_by, # PID or node
    :blocks_processed,
    :total_blocks,
    :message     # Current status message
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
      cond do
        total_blocks <= 0 -> 0
        blocks_processed >= total_blocks -> 100.0
        true -> Float.round(blocks_processed / total_blocks * 100, 1)
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
        :started ->
          {now, nil, nil}

        :running ->
          {task.started_at || now, nil, nil}

        :completed ->
          started = task.started_at || now
          {started, now, DateTime.diff(now, started, :second)}

        :failed ->
          started = task.started_at || now
          {started, now, DateTime.diff(now, started, :second)}

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
  def mark_started(task, message \\ "Starting operation"),
    do: mark_status(task, :started, message)

  def mark_running(task, message \\ "Operation in progress"),
    do: mark_status(task, :running, message)

  def mark_completed(task, message \\ "Completed"),
    do: mark_status(task, :completed, message)

  def mark_failed(task, reason, message \\ "Operation failed"),
    do: mark_status(task, :failed, message, reason)
end
