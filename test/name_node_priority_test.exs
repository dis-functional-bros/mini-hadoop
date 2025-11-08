defmodule MiniHadoop.Master.NameNodePriorityTest do
  use ExUnit.Case

  alias MiniHadoop.Master.NameNode

  test "assign blocks prioritizes datanodes with lowest load" do
    # setup 3 datanode dengan load berbeda
    datanodes = %{
      "nodeA" => %{pid: self(), blocks: ["b1","b2","b3"], last_heartbeat: DateTime.utc_now()},
      "nodeB" => %{pid: self(), blocks: ["b4"], last_heartbeat: DateTime.utc_now()},
      "nodeC" => %{pid: self(), blocks: [], last_heartbeat: DateTime.utc_now()}
    }

    block_ids = ["new_block_1","new_block_2","new_block_3","new_block_4", "new_block_5", "new_block_6"]

    assignments =
      :erlang.apply(NameNode, :assign_blocks_to_datanodes, [block_ids, datanodes, 1])

    # cek urutan sesuai priority:
    # nodeC load 0 dulu, lalu nodeB load 1, lalu nodeA load 3
    assert assignments["new_block_1"] == ["nodeC"] # Kapasitas tiap node saat ini: C=1, B=1, A=3
    assert assignments["new_block_2"] == ["nodeB"] # Kapasitas tiap node saat ini: C=1, B=2, A=3
    assert assignments["new_block_3"] == ["nodeC"] # Kapasitas tiap node saat ini: C=2, B=2, A=3
    # round berikutnya nodeB karena B dan C seri, ditentukan secara leksikografis
    assert assignments["new_block_4"] == ["nodeB"] # Kapasitas tiap node saat ini: C=2, B=3, A=3
    assert assignments["new_block_5"] == ["nodeC"] # Kapasitas tiap node saat ini: C=3, B=3, A=3
    assert assignments["new_block_6"] == ["nodeA"] # Kapasitas tiap node saat ini: C=3, B=3, A=4
  end
end
