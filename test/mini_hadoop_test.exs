defmodule MiniHadoopTest do
  use ExUnit.Case

  describe "API availability" do
    test "MiniHadoop module exists" do
      assert Code.ensure_loaded?(MiniHadoop)
    end

    test "has store_file function" do
      assert function_exported?(MiniHadoop, :store_file, 2)
    end

    test "has read_file function" do
      assert function_exported?(MiniHadoop, :read_file, 1)
    end

    test "has delete_file function" do
      assert function_exported?(MiniHadoop, :delete_file, 1)
    end

    test "has list_files function" do
      assert function_exported?(MiniHadoop, :list_files, 0)
    end

    test "has cluster_info function" do
      assert function_exported?(MiniHadoop, :cluster_info, 0)
    end
  end
end
