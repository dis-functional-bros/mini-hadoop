FROM elixir:1.17-alpine

# Install build dependencies
RUN apk add --no-cache build-base git

# Create app directory
WORKDIR /app

# Install hex and rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Copy mix files
COPY mix.exs ./

# Install dependencies
RUN mix deps.get && mix deps.compile

# Copy application code
COPY . .

# Compile the application
RUN mix compile

# Expose ports
EXPOSE 4000

# Start the application
CMD ["mix", "run", "--no-halt"]
