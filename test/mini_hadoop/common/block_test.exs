defmodule MiniHadoop.Common.BlockTest do
  use ExUnit.Case
  alias MiniHadoop.Common.Block

  describe "split_into_blocks/2" do
    test "splits small data into single block" do
      data = "Hello, World!"
      blocks = Block.split_into_blocks(data)
      
      assert length(blocks) == 1
      assert hd(blocks).data == data
    end

    test "splits large data into multiple blocks" do
      # Create data larger than default block size
      large_data = String.duplicate("a", 65 * 1024 * 1024)  # 65MB
      blocks = Block.split_into_blocks(large_data)
      
      assert length(blocks) > 1
    end

    test "each block has an id" do
      data = "Test data"
      blocks = Block.split_into_blocks(data)
      
      Enum.each(blocks, fn block ->
        assert is_integer(block.id)
      end)
    end

    test "each block has size" do
      data = "Test data with size"
      blocks = Block.split_into_blocks(data)
      
      Enum.each(blocks, fn block ->
        assert block.size == byte_size(block.data)
      end)
    end

    test "each block has a checksum" do
      data = "Test data with checksum"
      blocks = Block.split_into_blocks(data)
      
      Enum.each(blocks, fn block ->
        assert is_binary(block.checksum)
        assert String.length(block.checksum) == 32  # MD5 hex length
      end)
    end

    test "can use custom block size" do
      data = String.duplicate("x", 1000)
      custom_size = 100
      blocks = Block.split_into_blocks(data, custom_size)
      
      assert length(blocks) == 10
    end

    test "blocks are sequential" do
      data = "0123456789"
      blocks = Block.split_into_blocks(data, 2)
      
      assert length(blocks) == 5
      assert Enum.at(blocks, 0).data == "01"
      assert Enum.at(blocks, 1).data == "23"
      assert Enum.at(blocks, 2).data == "45"
      assert Enum.at(blocks, 3).data == "67"
      assert Enum.at(blocks, 4).data == "89"
    end
  end

  describe "verify_checksum/1" do
    test "verifies correct checksum" do
      data = "Valid data"
      [block] = Block.split_into_blocks(data)
      
      assert Block.verify_checksum(block) == true
    end

    test "detects corrupted data" do
      data = "Original data"
      [block] = Block.split_into_blocks(data)
      
      # Corrupt the block data
      corrupted_block = %{block | data: "Corrupted data"}
      
      assert Block.verify_checksum(corrupted_block) == false
    end

    test "verifies all blocks" do
      data = String.duplicate("test", 1000)
      blocks = Block.split_into_blocks(data, 100)
      
      assert Enum.all?(blocks, &Block.verify_checksum/1)
    end
  end
end
