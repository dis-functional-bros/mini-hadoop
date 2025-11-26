defmodule MiniHadoop.ExecuteMapTest do
  use ExUnit.Case

  alias MiniHadoop.ComputeTask.TaskRunner
  alias MiniHadoop.Map.Examples.WordCount

  test "execute_map_task directly with WordCount example" do
    input = "hello world hello"

    # Call the function directly (now public)
    result = TaskRunner.execute_map_task(input, WordCount, %{})

    # Verify the result is grouped by key with values as lists
    assert result == %{
      "hello" => [2],
      "world" => [1]
    }
  end
end
