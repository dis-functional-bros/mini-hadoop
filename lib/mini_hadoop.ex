defmodule MiniHadoop do
  defdelegate store_file(filename, content), to: MiniHadoop.Client
  defdelegate read_file(filename), to: MiniHadoop.Client
  defdelegate delete_file(filename), to: MiniHadoop.Client
  defdelegate list_files(), to: MiniHadoop.Client
  defdelegate cluster_info(), to: MiniHadoop.Client
end
