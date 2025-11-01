defmodule MiniHadoop.Common.Task do
  @moduledoc """
  Task data structure for individual map/reduce tasks.
  
  NOTE: This is a skeleton implementation. MapReduce functionality not yet implemented.
  """
  
  defstruct [
    :id,
    :job_id,
    :type,        # :map or :reduce
    :input,
    :output,
    :status,      # :pending, :running, :completed, :failed
    :attempts,
    :assigned_to
  ]

  def new(attrs) do
    struct(__MODULE__, Map.merge(%{status: :pending, attempts: 0}, attrs))
  end
end
