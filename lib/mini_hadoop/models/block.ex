defmodule MiniHadoop.Models.Block do
  @moduledoc """
  Block operations and utilities for the distributed file system.
  """

  @block_size Application.compile_env(:mini_hadoop, :block_size, 64 * 1024) # 64KB default

  defstruct [
    :id,
    :data,
    :size,
    :checksum
  ]

  def get_block_size, do: @block_size

  @doc """
  Utility function to calculate number of blocks for a file.
  """
  def calculate_num_blocks(file_size, block_size \\ @block_size) do
    ceil(file_size / block_size)
  end
end
