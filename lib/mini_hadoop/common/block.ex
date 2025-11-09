defmodule MiniHadoop.Common.Block do
  @moduledoc """
  Block operations and utilities for the distributed file system.
  Now simplified for fixed-size streaming chunks.
  """

  # 64KB for testing (you can change to 64MB later)
  @block_size 4 * 1024

  # We don't need the Block struct anymore since we're just using raw binary chunks
  # But keep it if other parts of the system use it, or remove it entirely

  defstruct [
    :id,
    :data,
    :size,
    :checksum
  ]

  def get_block_size, do: @block_size

  # Remove all splitter functions since we don't need them anymore
  # The File.stream! with fixed chunk size handles the "splitting"

  @doc """
  Utility function to calculate number of blocks for a file.
  """
  def calculate_num_blocks(file_size, block_size \\ @block_size) do
    ceil(file_size / block_size)
  end
end
