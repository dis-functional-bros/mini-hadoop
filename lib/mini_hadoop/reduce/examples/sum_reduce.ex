defmodule MiniHadoop.Reduce.Examples.SumReduce do
  @behaviour MiniHadoop.Reduce

  @impl true

  def reduce(data, _context) when is_map(data) do
    Enum.into(data, [], fn {key, values} ->
      {key, Enum.sum(values)}
    end)
  end
end
