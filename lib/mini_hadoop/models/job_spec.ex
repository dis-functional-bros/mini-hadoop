# lib/mini_hadoop/models/job.ex
defmodule MiniHadoop.Models.JobSpec do
  @moduledoc """
  Represents a complete MapReduce job specification (static plan).
  Contains config to execute a job.
  """
  require Logger
  alias MiniHadoop.Types

  defstruct [
    :id,
    :job_name,
    :input_files,
    :output_dir,
    :map_function,
    :map_context,
    :reduce_function,
    :reduce_context
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          job_name: String.t(),
          input_files: [String.t()],
          output_dir: String.t(),
          map_function: Types.map_function(),
          map_context: map(),
          reduce_function: Types.reduce_function(),
          reduce_context: map()
        }

  @spec create(Keyword.t()) :: {:ok, t()} | {:error, String.t()}
  def create(attrs) when is_list(attrs) do
    with {:ok, validated_attrs} <- validate_keyword_list(attrs),
         {:ok, validated_attrs} <- validate_input_files(validated_attrs),
         attrs_map <- Map.new(validated_attrs),
         {:ok, job_id} <- generate_job_id(),
         {:ok, normalized_attrs} <- normalize_functions_to_arity_2(attrs_map) do
      defaults = %{
        id: job_id,
        map_context: %{},
        reduce_context: %{}
      }

      {:ok, struct(__MODULE__, Map.merge(defaults, normalized_attrs))}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_keyword_list(attrs) do
    with :ok <- validate_allowed_keys(attrs),
         :ok <- validate_required_keys(attrs),
         :ok <- validate_key_types(attrs) do
      {:ok, attrs}
    end
  end

  defp validate_input_files(attrs) do
    missing_files = Enum.reject(attrs[:input_files], &MiniHadoop.Master.MasterNode.filename_exists/1)

    if Enum.empty?(missing_files) do
      {:ok, attrs}
    else
      {:error, "Input files do not exist: #{Enum.join(missing_files, ", ")}"}
    end
  end

  defp validate_allowed_keys(attrs) do
    allowed_keys = [
      :job_name,
      :input_files,
      :output_dir,
      :map_function,
      :reduce_function,
      :map_context,
      :reduce_context
    ]

    invalid_keys = Enum.reject(attrs, fn {key, _} -> key in allowed_keys end)
                   |> Enum.map(&elem(&1, 0))

    if Enum.empty?(invalid_keys) do
      :ok
    else
      {:error, "Invalid keys: #{Enum.join(invalid_keys, ", ")}. Allowed keys: #{Enum.join(allowed_keys, ", ")}"}
    end
  end

  defp validate_required_keys(attrs) do
    required_keys = [:job_name, :input_files, :map_function, :reduce_function]
    attrs_map = Map.new(attrs)

    missing_keys = Enum.filter(required_keys, & !Map.has_key?(attrs_map, &1))

    if Enum.empty?(missing_keys) do
      :ok
    else
      {:error, "Missing required keys: #{Enum.join(missing_keys, ", ")}"}
    end
  end

  defp validate_key_types(attrs) do
    Enum.reduce_while(attrs, :ok, fn
      {:job_name, value}, _acc when is_binary(value) ->
        {:cont, :ok}
      {:job_name, value}, _acc ->
        {:halt, {:error, "job_name must be a string, got: #{inspect(value)}"}}

      {:input_files, value}, _acc when is_list(value) ->
        {:cont, :ok}
      {:input_files, value}, _acc ->
        {:halt, {:error, "input_files must be a list, got: #{inspect(value)}"}}

      {:output_dir, value}, _acc when is_binary(value) ->
        {:cont, :ok}
      {:output_dir, value}, _acc ->
        {:halt, {:error, "output_dir must be a string, got: #{inspect(value)}"}}

      {:map_function, value}, _acc when is_function(value) ->
        {:cont, :ok}
      {:map_function, value}, _acc ->
        {:halt, {:error, "map_function must be a function, got: #{inspect(value)}"}}

      {:reduce_function, value}, _acc when is_function(value) ->
        {:cont, :ok}
      {:reduce_function, value}, _acc ->
        {:halt, {:error, "reduce_function must be a function, got: #{inspect(value)}"}}

      {:map_context, value}, _acc when is_map(value) ->
        {:cont, :ok}
      {:map_context, value}, _acc ->
        {:halt, {:error, "map_context must be a map, got: #{inspect(value)}"}}

      {:reduce_context, value}, _acc when is_map(value) ->
        {:cont, :ok}
      {:reduce_context, value}, _acc ->
        {:halt, {:error, "reduce_context must be a map, got: #{inspect(value)}"}}

      {key, _value}, _acc ->
        {:halt, {:error, "Unexpected key during type validation: #{key}"}}
    end)
  end

  defp generate_job_id do
    {:ok, "job_#{generate_id()}"}
  end

  defp generate_id do
    :crypto.strong_rand_bytes(2) |> Base.encode16(case: :lower)
  end

  defp normalize_functions_to_arity_2(attrs) do
    map_function = Map.get(attrs, :map_function)
    reduce_function = Map.get(attrs, :reduce_function)

    with {:ok, normalized_map_fn} <- normalize_function(map_function, :map),
         {:ok, normalized_reduce_fn} <- normalize_function(reduce_function, :reduce) do
      {:ok, %{
        attrs
        | map_function: normalized_map_fn,
          reduce_function: normalized_reduce_fn
      }}
    end
  end

  defp normalize_function(function, type) do
    case :erlang.fun_info(function, :arity) do
      {:arity, 1} ->
        # Wrap 1-arity function to accept but ignore context
        normalized_fn = fn data, _context -> function.(data) end
        {:ok, normalized_fn}

      {:arity, 2} ->
        {:ok, function}

      arity ->
        {:error, "#{type} function has invalid arity: #{arity}. Must be 1 or 2."}
    end
  end

  # Public function to get the normalized functions (read-only)
  @spec get_normalized_functions(t()) :: {Types.map_function(), Types.reduce_function()}
  def get_normalized_functions(job) do
    {job.map_function, job.reduce_function}
  end
end
