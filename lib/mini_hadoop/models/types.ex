defmodule MiniHadoop.Models.Types do
  @moduledoc """
  Basic types used throughout the MiniHadoop application.
  """

  @type key :: any()
  @type value :: any()
  @type status :: :pending | :running | :completed | :failed
  @type result :: [{key(), value()}]
  @type map_function :: (binary(), any() -> result())
  @type reduce_function :: (map(), any() -> result())
  @type context :: map()
  @type task_type :: :map | :reduce
  @type progress :: %{
    map: {completed :: non_neg_integer(), total :: non_neg_integer()},
    reduce: {completed :: non_neg_integer(), total :: non_neg_integer()}
  }
  @type sort_result_opt :: {by :: :key | :value, direction :: :asc | :desc}

end
