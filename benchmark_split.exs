# benchmark_split.exs

# Configuration
file_size_mb = 100
block_size = 64 * 1024 # 64KB
line_length = 100 # Average line length
filename = "bench_test_file.txt"

IO.puts("Generating #{file_size_mb}MB test file...")
File.write!(filename, "")
file = File.open!(filename, [:write, :append])

chunk = String.duplicate("a", line_length) <> "\n"
# Write in larger chunks to speed up generation
write_chunk = String.duplicate(chunk, 1000)
total_writes = div(file_size_mb * 1024 * 1024, byte_size(write_chunk))

Enum.each(1..total_writes, fn _ -> IO.write(file, write_chunk) end)
File.close(file)

IO.puts("File generated. Starting benchmark...")

# 1. Binary Split (Original)
{time_binary, _} = :timer.tc(fn ->
  File.stream!(filename, block_size, [:read, :binary])
  |> Stream.run()
end)

IO.puts("Binary Split Time: #{time_binary / 1_000_000} seconds")

# 2. Newline Split (New)
{time_newline, _} = :timer.tc(fn ->
  File.stream!(filename, [:read])
  |> Stream.chunk_while(
    {[], 0},
    fn line, {acc, current_size} ->
      line_size = byte_size(line)
      new_size = current_size + line_size

      if new_size > block_size and current_size > 0 do
        chunk_bin = IO.iodata_to_binary(Enum.reverse(acc))
        {:cont, chunk_bin, {[line], line_size}}
      else
        {:cont, {[line | acc], new_size}}
      end
    end,
    fn
      {[], _} -> {:cont, []}
      {acc, _} -> {:cont, IO.iodata_to_binary(Enum.reverse(acc)), []}
    end
  )
  |> Stream.run()
end)

IO.puts("Newline Split Time: #{time_newline / 1_000_000} seconds")

ratio = time_newline / time_binary
IO.puts("Ratio (Newline / Binary): #{Float.round(ratio, 2)}x slower")

# Cleanup
File.rm(filename)
