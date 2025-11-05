defmodule MiniHadoop.Common.Block do
  @moduledoc """
  Block data structure and utilities.
  """

  defstruct [:id, :data, :size, :checksum]

  @block_size 64 * 1024 * 1024  # 64MB default


  # TODO: Change the function to use higher order function for splitting with structure logic

  def split_into_blocks(data, block_size \\ @block_size) do
    data
    |> :binary.bin_to_list()
    |> Enum.chunk_every(block_size)
    |> Enum.with_index()
    |> Enum.map(fn {chunk, idx} ->
      binary_chunk = :binary.list_to_bin(chunk)
      %__MODULE__{
        id: idx,
        data: binary_chunk,
        size: byte_size(binary_chunk),
        checksum: compute_checksum(binary_chunk)
      }
    end)
  end

  def verify_checksum(%__MODULE__{} = block) do
    computed = compute_checksum(block.data)
    computed == block.checksum
  end

  defp compute_checksum(data) do
    :crypto.hash(:md5, data) |> Base.encode16()
  end
end
