defmodule MiniHadoop.Examples.WordCountTest do
  use ExUnit.Case

  alias MiniHadoop.Examples.WordCount

  describe "word_count_mapper/2" do
    test "splits line into words and maps to tuples" do
      result = WordCount.word_count_mapper("hello world hello", %{})

      assert is_list(result)
      assert {"hello", 1} in result
      assert {"world", 1} in result
    end

    test "handles case insensitivity" do
      result = WordCount.word_count_mapper("Hello WORLD hello", %{})

      # Should be lowercased
      assert Enum.any?(result, fn {k, _v} -> k == "hello" end)
    end

    test "removes punctuation" do
      result = WordCount.word_count_mapper("Hello, world! Hello?", %{})

      # Should not contain punctuation
      assert not Enum.any?(result, fn {k, _v} -> String.contains?(k, [",", "!", "?"]) end)
    end

    test "handles empty string" do
      result = WordCount.word_count_mapper("", %{})
      assert result == []
    end

    test "handles whitespace only" do
      result = WordCount.word_count_mapper("   \n  \t  ", %{})
      assert result == [] or Enum.all?(result, fn {k, _v} -> String.trim(k) == "" end)
    end
  end

  describe "word_count_reducer/2" do
    test "sums up word counts" do
      result = WordCount.word_count_reducer({"word", [1, 1, 1, 1]}, %{})

      assert result == {"word", 4}
    end

    test "handles single count" do
      result = WordCount.word_count_reducer({"word", [1]}, %{})

      assert result == {"word", 1}
    end

    test "handles empty list" do
      result = WordCount.word_count_reducer({"word", []}, %{})

      assert result == {"word", 0}
    end

    test "handles multiple different counts" do
      result = WordCount.word_count_reducer({"word", [5, 3, 2]}, %{})

      assert result == {"word", 10}
    end
  end

  describe "integration: mapper -> reducer" do
    test "maps and reduces word count correctly" do
      line = "apple banana apple cherry apple"

      map_result = WordCount.word_count_mapper(line, %{})
      # Group by key
      grouped =
        map_result
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

      # Reduce
      reduce_results =
        grouped
        |> Enum.map(fn {key, values} ->
          WordCount.word_count_reducer({key, values}, %{})
        end)

      reduce_map = Enum.into(reduce_results, %{})

      assert reduce_map["apple"] == 3
      assert reduce_map["banana"] == 1
      assert reduce_map["cherry"] == 1
    end
  end
end
