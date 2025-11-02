defmodule MiniHadoop.Master.NameNodeDownTest do
  use ExUnit.Case, async: false
  alias MiniHadoop.Master.NameNode

  setup do
    {:ok, _pid} = start_supervised(NameNode)
    :ok
  end

  test "removes datanode when monitored process goes down" do
    datanode_pid = spawn(fn -> receive do _ -> :ok end end)

    assert :ok = NameNode.register_datanode("temp_datanode", datanode_pid)

    datanodes_before = NameNode.get_datanodes()
    assert Enum.any?(datanodes_before, fn dn -> dn.hostname == "temp_datanode" end)

    Process.exit(datanode_pid, :kill)
    Process.sleep(50)

    datanodes_after = NameNode.get_datanodes()
    refute Enum.any?(datanodes_after, fn dn -> dn.hostname == "temp_datanode" end)
  end
end
