defmodule MiniHadoop.Map.MapBehaviour do
  @moduledoc """
  Behaviour and helpers for running Map-style functions across blocks/partitions.

  The simplest workflow:

    * Implement `c:map/2` for your module.
    * Call `MiniHadoop.Map.execute/3` with the module, binary block data, and
      an optional context map (metadata, params, etc).
    * Use the helpers from tasks/workers to uniformly handle errors.
  """

  @typedoc "Result returned by a Map implementation."
  @type result :: [{key :: term(), value :: term()}]

  @callback map(data :: binary(), any()) :: result

  @doc """
  Executes the provided map module with binary data and an optional context.

  Returns `{:ok, result}` when execution succeeds or `{:error, reason}` when
  the module is missing, does not implement `c:map/2`, or raises while running.
  """
  @spec execute(module(), binary(), any()) :: {:ok, result} | {:error, term()}
  def execute(map_module, data, context \\ %{}) when is_binary(data) do
    with :ok <- ensure_valid_module(map_module) do
      do_execute(map_module, data, context)
    end
  end

  defp do_execute(map_module, data, context) do
    {:ok, map_module.map(data, context)}
  rescue
    error ->
      {:error, {:map_failed, Exception.message(error)}}
  catch
    kind, reason ->
      {:error, {:map_failed, {kind, reason}}}
  end

  defp ensure_valid_module(map_module) when is_atom(map_module) do
    with {:module, _} <- Code.ensure_loaded(map_module),
         true <- function_exported?(map_module, :map, 2) do
      :ok
    else
      {:error, :nofile} ->
        {:error, {:module_not_loaded, map_module}}

      {:error, reason} ->
        {:error, reason}

      false ->
        {:error, {:missing_callback, map_module}}
    end
  end

  defp ensure_valid_module(module), do: {:error, {:invalid_module, module}}
end
