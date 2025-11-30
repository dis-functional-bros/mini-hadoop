defmodule MiniHadoop.Examples.PageRank do
  @moduledoc """
    A map-reduce example for word count using MiniHadoop.MapReduce.
  """
  def word_count_mapper(data, context) when is_binary(data) do
    try do
      tokenizer = Map.get(context, :tokenizer, &default_tokenizer/1)
      result = data
          |> tokenizer.()
          |> Enum.frequencies()
          |> Enum.to_list()

      {:ok, result}
    rescue
      e -> {:error, e}
    end
  end

  # Mengubah jadi list kata
  defp default_tokenizer(binary) do
    binary
    |> String.downcase()
    |> String.replace(~r/[^[:alnum:]\s]/u, " ")
    |> String.split(~r/\s+/, trim: true)
  end

  def word_count_reducer(data, _context) when is_map(data) do
    try do
      result = Enum.into(data, [], fn {key, values} ->
        {key, Enum.sum(values)}
      end)

      {:ok, result}
    rescue
      e -> {:error, e}
    end
  end

end
