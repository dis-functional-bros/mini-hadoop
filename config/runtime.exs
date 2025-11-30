import Config

# Runtime configuration - uses actual environment variables
config :mini_hadoop,
  node_type: System.get_env("NODE_TYPE", "worker"),
  master_node: System.get_env("MASTER_NODE", "master@master"),
  node_name: System.get_env("NODE_NAME", "unnamed_node"),
  data_base_path: System.get_env("DATA_BASE_PATH", "/app/data"),
  temp_path: System.get_env("DATA_TEMP_PATH", "/app/temp"),
  retrieve_result_path: System.get_env("RETRIVE_RESULT_PATH", "/app/retrieve")
