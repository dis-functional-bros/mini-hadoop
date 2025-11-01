ExUnit.start()

# Exclude integration tests by default
# Run with: mix test --include integration
ExUnit.configure(exclude: [integration: true])
