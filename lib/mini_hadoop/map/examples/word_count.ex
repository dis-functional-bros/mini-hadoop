defmodule MiniHadoop.Map.Examples.WordCount do
  @moduledoc """
  Fungsi map yang menghitung frekuensi kata dalam teks.
  """
  @behaviour MiniHadoop.Map.MapBehaviour

  @impl true
  def map(data, context) when is_binary(data) do
    tokenizer = Map.get(context, :tokenizer, &default_tokenizer/1)

    data
    |> tokenizer.()
    |> Enum.frequencies()
  end

  # Mengubah jadi list kata
  defp default_tokenizer(binary) do
    binary
    |> String.downcase()
    |> String.replace(~r/[^[:alnum:]\s]/u, " ")
    |> String.split(~r/\s+/, trim: true)
  end
end
