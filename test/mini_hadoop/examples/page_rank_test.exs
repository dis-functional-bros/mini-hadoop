defmodule MiniHadoop.Examples.PageRankTest do
  use ExUnit.Case

  alias MiniHadoop.Examples.PageRank

  describe "pagerank_mapper/2" do
    test "distributes page rank to outbound links" do
      line = "page1\tpage2 page3"
      context = %{damping_factor: 0.85, total_pages: 10}

      result = PageRank.pagerank_mapper(line, context)

      assert is_list(result)
      # Should have baseline for source + contributions for targets
      assert length(result) == 3
    end

    test "handles page with single outbound link" do
      line = "source\ttarget"
      context = %{damping_factor: 0.85, total_pages: 100}

      result = PageRank.pagerank_mapper(line, context)

      assert is_list(result)
      assert length(result) == 2
    end

    test "handles page with multiple outbound links" do
      line = "hub\tpage1 page2 page3 page4 page5"
      context = %{damping_factor: 0.85, total_pages: 1000}

      result = PageRank.pagerank_mapper(line, context)

      # Should have 1 baseline + 5 contributions
      assert length(result) == 6
    end

    test "uses default damping factor" do
      line = "page1\tpage2"
      context = %{total_pages: 10}

      # Should not raise error
      result = PageRank.pagerank_mapper(line, context)
      assert is_list(result)
    end

    test "respects damping factor in distribution" do
      line = "page1\tpage2 page3"
      context = %{damping_factor: 0.85, total_pages: 1}

      result = PageRank.pagerank_mapper(line, context)

      # All results should be non-negative
      assert Enum.all?(result, fn {_page, rank} -> rank >= 0 end)
    end
  end

  describe "pagerank_reducer/2" do
    test "sums incoming rank contributions" do
      incoming_ranks = {"page1", [0.1, 0.2, 0.15]}

      result = PageRank.pagerank_reducer(incoming_ranks, %{})

      assert result == {"page1", 0.45}
    end

    test "handles single contribution" do
      incoming_ranks = {"page", [0.5]}

      result = PageRank.pagerank_reducer(incoming_ranks, %{})

      assert result == {"page", 0.5}
    end

    test "handles empty contributions" do
      incoming_ranks = {"page", []}

      result = PageRank.pagerank_reducer(incoming_ranks, %{})

      assert result == {"page", 0}
    end

    test "handles multiple contributions" do
      incoming_ranks = {"page", [0.1, 0.2, 0.3, 0.4]}

      result = PageRank.pagerank_reducer(incoming_ranks, %{})

      assert result == {"page", 1.0}
    end
  end

  describe "integration: mapper -> reducer" do
    test "maps and reduces page rank correctly" do
      lines = [
        "page1\tpage2 page3",
        "page2\tpage3",
        "page3\tpage1"
      ]

      context = %{damping_factor: 0.85, total_pages: 3}

      # Map phase
      map_results =
        lines
        |> Enum.flat_map(&PageRank.pagerank_mapper(&1, context))

      # Group by key
      grouped =
        map_results
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

      # Reduce phase
      reduce_results =
        grouped
        |> Enum.map(fn {key, values} ->
          PageRank.pagerank_reducer({key, values}, context)
        end)

      result_map = Enum.into(reduce_results, %{})

      # All pages should have some rank
      assert Map.has_key?(result_map, "page1")
      assert Map.has_key?(result_map, "page2")
      assert Map.has_key?(result_map, "page3")

      # All ranks should be positive
      assert Enum.all?(result_map, fn {_page, rank} -> rank > 0 end)
    end
  end
end
