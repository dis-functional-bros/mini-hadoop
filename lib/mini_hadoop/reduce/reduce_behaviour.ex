defmodule MiniHadoop.Reduce.ReduceBehaviour do
  @moduledoc """
  Behaviour and helpers for running Reduce-style functions across grouped key-value lists.

  Typical workflow:

    * Implement `c:reduce/2` for your module.
    * Call `MiniHadoop.Reduce.execute/3` with the module, grouped map data, and a context map.
    * Use this inside your Reduce workers or tasks for uniform error handling.
  """

  @typedoc "Result returned by a Reduce implementation."
  @type result :: [{key :: term(), value :: term()}]

  @callback reduce(data :: map(), any()) :: result

  # --- Public entrypoint ---
  @spec execute(module(), map(), any()) :: {:ok, result} | {:error, term()}
  def execute(reduce_module, data, context \\ %{}) when is_map(data) do
    with :ok <- ensure_valid_module(reduce_module) do
      do_execute(reduce_module, data, context)
    end
  end

  # --- Internal reducer executor ---
  defp do_execute(reduce_module, data, context) do
    {:ok, reduce_module.reduce(data, context)}
  rescue
    error ->
      {:error, {:reduce_failed, Exception.message(error)}}

  catch
    kind, reason ->
      {:error, {:reduce_failed, {kind, reason}}}
  end

  # --- Module validation ---
  defp ensure_valid_module(reduce_module) when is_atom(reduce_module) do
    with {:module, _} <- Code.ensure_loaded(reduce_module),
         true <- function_exported?(reduce_module, :reduce, 2) do
      :ok
    else
      {:error, :nofile} ->
        {:error, {:module_not_loaded, reduce_module}}

      {:error, reason} ->
        {:error, reason}

      false ->
        {:error, {:missing_callback, reduce_module}}
    end
  end

  defp ensure_valid_module(module),
    do: {:error, {:invalid_module, module}}
end
