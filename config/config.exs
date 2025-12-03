import Config

# Common config for ALL environments
config :mini_hadoop,
  heartbeat_interval_ms: 50_000,
  block_size: 16 * 1024 * 1024,
  batch_size: 10,
  block_replication_factor: 2,
  max_concurrent_jobs: 1,
  max_queue_size_of_jobs: 10,
  max_concurrent_tasks_on_runner: 1,
  max_num_of_key_each_reduce_task: 500

# Environment-specific config (compiled)
# import_config "#{config_env()}.exs"
