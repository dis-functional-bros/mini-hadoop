defmodule MiniHadoop.Examples.PageRank do
  @moduledoc """
  PageRank implementation for MiniHadoop.

  Processes adjacency lists: "source\\ttarget1 target2 ..."
  Output: [{page, rank}, ...] sorted by rank descending.
  """

  @default_damping_factor 0.85
  @default_initial_rank 1.0

  require Logger

  @doc """
  PageRank mapper - distributes page rank to outgoing links.

  Context may contain:
  - `:pagerank_file`: Path to JSON with previous ranks {"page": rank, ...}
  - `:damping_factor`: Optional, defaults to 0.85
  - `:total_pages`: Optional, defaults to 1,000,000

  If no pagerank_file exists, uses default rank 1.0 for all pages.

  Output: [{page, contribution}, ...]

  Strategy: For each line "source target1 target2 ...":
  1. Source gets baseline (1-d)/n
  2. Source contributes d*(rank/out_degree) to each target
  3. Targets only get the distributed rank (NOT baseline)
  """
  def pagerank_mapper(data, context) when is_binary(data) do
    try do
      pagerank_file = Map.get(context, :pagerank_file)
      d = Map.get(context, :damping_factor, @default_damping_factor)
      n = Map.get(context, :total_pages, 1_000_000)

      # Load previous ranks or use empty map (do this once)
      page_ranks =
        if pagerank_file && File.exists?(pagerank_file) do
          load_ranks(pagerank_file)
        else
          %{}
        end

      # Calculate baseline contribution
      baseline = (1 - d) / n

      # Track unique page IDs for logging
      unique_page_ids = MapSet.new()
      source_count = 0
      target_count = 0

      # Process data line by line lazily
      contributions =
        data
        |> String.splitter("\n", trim: true)
        |> Stream.flat_map(fn line ->
          case parse_line(line) do
            {:ok, source, targets} ->
              # Track unique page IDs
              current_unique = MapSet.new([source | targets])
              source_count = if targets != [], do: source_count + 1, else: source_count
              target_count = target_count + length(targets)

              # Get source rank (default to 1.0 if not found)
              rank = Map.get(page_ranks, source, @default_initial_rank)

              # Source gets baseline
              source_baseline = {source, baseline}

              if targets != [] do
                # Source has outgoing links: distribute d*rank to targets
                rank_per_target = d * (rank / length(targets))
                target_contributions = Enum.map(targets, fn target ->
                  {target, rank_per_target}
                end)

                # Combine source baseline + distributed rank to targets
                [source_baseline | target_contributions]
              else
                # Source has no outgoing links: only baseline
                [source_baseline]
              end

            {:error, reason} ->
              Logger.error("Failed to parse line: #{inspect(String.slice(line, 0, 100))}... Reason: #{inspect(reason)}")
              []
          end
        end)
        |> Enum.to_list()

      {:ok, contributions}
    rescue
      error ->
        Logger.error("Mapper error: #{inspect(error)}. Stacktrace: #{inspect(__STACKTRACE__)}")
        {:error, error}
    end
  end

  @doc """
  PageRank reducer - sums contributions.

  Context may contain:
  - `:damping_factor`: Optional, defaults to 0.85
  - `:total_pages`: Optional, defaults to 1,000,000

  Input: %{page_id => [contributions]}
  Output: [{page, rank}, {page, rank}, ...] sorted descending by rank

  Note: Mapper already includes (1-d)/n, so reducer just sums contributions.
  """
  def pagerank_reducer(data, context) when is_map(data) do
    try do
      # Calculate new ranks by summing contributions
      results =
        data
        |> Enum.map(fn {page, contributions} ->
          sum = Enum.sum(contributions)
          {page, sum}
        end)
        |> Enum.sort_by(fn {_page, rank} -> -rank end)  # Sort descending by rank

      # Log tuple count result
      Logger.info("PageRank reducer generated #{length(results)} tuples")

      {:ok, results}  # Return list of tuples [{page, rank}, ...]
    rescue
      error ->
        Logger.error("Reducer error: #{inspect(error)}. Stacktrace: #{inspect(__STACKTRACE__)}")
        {:error, error}
    end
  end

  # Private helper functions

  defp parse_line(line) do
    line = String.trim(line)

    if String.length(line) == 0 do
      {:error, :empty_line}
    else
      case String.split(line, "\t") do
        [source, targets_str] ->
          targets = String.split(targets_str, " ", trim: true)
          {:ok, source, targets}

        [source] ->
          {:ok, source, []}

        _ ->
          {:error, :bad_format}
      end
    end
  end

  defp load_ranks(file) do
    try do
      case File.read(file) do
        {:ok, content} ->
          case Jason.decode(content) do
            {:ok, rank_map} when is_map(rank_map) ->
              # The file is already in the format we need: %{"page" => rank}
              # Just need to ensure all values are numbers
              Enum.reduce(rank_map, %{}, fn
                {page, rank} when is_number(rank) ->
                  Map.put(%{}, page, rank)
                {page, rank} when is_binary(rank) ->
                  # Try to convert string to number
                  case Float.parse(rank) do
                    {parsed_rank, _} -> Map.put(%{}, page, parsed_rank)
                    :error -> %{}
                  end
                {page, _rank} -> %{}
              end)

            _ -> %{}
          end

        _ -> %{}
      end
    rescue
      error ->
        Logger.error("Error loading ranks from #{file}: #{inspect(error)}")
        %{}
    end
  end
end
