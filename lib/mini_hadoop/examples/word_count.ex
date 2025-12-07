defmodule MiniHadoop.Examples.WordCount do
  @moduledoc """
  Memory-efficient word count with boundary-aware streaming.
  """

  # Valid word characters (Unicode-aware)
  @valid_word_chars ~r/[\p{L}\p{Nd}_'-]/

  @doc """
  Main mapper using word-by-word streaming for maximum memory efficiency.
  """
  def word_count_mapper(data, _context) when is_binary(data) do
    try do
      counts =
        data
        |> String.downcase()
        |> stream_words()
        |> Enum.reduce(%{}, &count_word/2)

      {:ok, Map.to_list(counts)}
    rescue
      error -> {:error, error}
    end
  end

  @doc """
  Streams text word by word using a state machine approach.
  """
  def stream_words(text) when is_binary(text) do
    Stream.resource(
      # Start function: initialize with text and position
      fn -> {text, 0, byte_size(text)} end,

      # ]find next word
      fn
        {_, pos, total} when pos >= total ->
          {:halt, nil}

        {text, pos, total} = _state ->
          case next_word(text, pos, total) do
            {:word, word, new_pos} ->
              {[word], {text, new_pos, total}}
            :no_word ->
              {[], {text, total, total}}  # Skip to end
          end
      end,

      fn _ -> :ok end
    )
    |> Stream.filter(&valid_word?/1)
  end

  @doc """
  Finds the next word in text starting from given position.
  Returns {:word, word, new_position} or :no_word.
  """
  def next_word(text, start_pos, total) do
    # Step 1: Skip to first word character
    word_start = skip_to_word_start(text, start_pos, total)

    if word_start >= total do
      :no_word
    else
      word_end = find_word_end(text, word_start, total)

      word = binary_part(text, word_start, word_end - word_start)

      next_pos = skip_to_next_start(text, word_end, total)

      {:word, word, next_pos}
    end
  end

  defp skip_to_word_start(text, pos, total) when pos < total do
    <<_::binary-size(pos), char::utf8, _::binary>> = text

    if word_char?(char) do
      pos
    else
      skip_to_word_start(text, pos + byte_size(<<char::utf8>>), total)
    end
  end

  defp skip_to_word_start(_, pos, _), do: pos

  defp find_word_end(text, pos, total) when pos < total do
    <<_::binary-size(pos), char::utf8, _::binary>> = text

    if word_char?(char) do
      find_word_end(text, pos + byte_size(<<char::utf8>>), total)
    else
      pos
    end
  end

  defp find_word_end(_, pos, _), do: pos

  defp skip_to_next_start(text, pos, total) do
    skip_to_word_start(text, pos, total)
  end

  # Check if a Unicode codepoint is a valid word character
  defp word_char?(char) do
    # Letters, numbers, apostrophe, hyphen, underscore
    (char >= ?a and char <= ?z) or
    (char >= ?A and char <= ?Z) or
    (char >= ?0 and char <= ?9) or
    char == ?' or char == ?- or char == ?_ or
    # Unicode letter categories
    Regex.match?(@valid_word_chars, <<char::utf8>>)
  end

  # Helper functions
  defp count_word(word, acc) do
    Map.update(acc, word, 1, &(&1 + 1))
  end

  defp valid_word?(word) do
    byte_size(word) >= 2 and contains_letter?(word)
  end

  defp valid_word?("a"), do: true
  defp valid_word?("i"), do: true
  defp valid_word?(""), do: false

  defp contains_letter?(word) do
    String.match?(word, ~r/\p{L}/u)
  end

  @doc """
  Reducer with streaming support.
  """
  def word_count_reducer(data, _context) when is_map(data) do
    try do
      result =
        data
        |> Stream.map(fn {key, values} ->
          total = Enum.sum(Stream.filter(values, &is_integer/1))
          {key, total}
        end)
        |> Enum.to_list()

      {:ok, result}
    rescue
      error -> {:error, error}
    end
  end
end
