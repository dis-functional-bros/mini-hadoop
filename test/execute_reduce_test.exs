defmodule MiniHadoop.ExecuteReduceTest do
  use ExUnit.Case

  alias MiniHadoop.ComputeTask.TaskRunner
  alias MiniHadoop.Reduce.Examples.SumReduce

  test "execute_reduce_task with SumReduce example" do
    input = %{"dummy" => [4, 5, 7, 3], "data" => [1, 2, 3, 4], "more_data" => [5, 6, 7, 8]}

    # Call the function directly
    result = TaskRunner.execute_reduce_task(input, SumReduce, %{})

    # Verify the result
    expected = [
      {"dummy", 19},
      {"data", 10},
      {"more_data", 26}
    ]

    # Convert to map for easier assertion
    result_map = Enum.into(result, %{})
    expected_map = Enum.into(expected, %{})

    assert result_map == expected_map
  end
end
