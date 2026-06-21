defmodule Support.MessageTrace do
  # we should reimplement this with something nicer
  # like an in memory queue

  def save(message, trace_name) do
    File.write(path(trace_name), message, [:append])
  end

  def clear(trace_name) do
    File.rm_rf(path(trace_name))
  end

  def content(trace_name) do
    File.read!(path(trace_name))
  end

  # Like content/1, but returns "" when the trace file does not exist yet.
  # Traces are written asynchronously, so the file may be missing on early reads.
  def read(trace_name) do
    case File.read(path(trace_name)) do
      {:ok, content} -> content
      {:error, :enoent} -> ""
    end
  end

  defp path(trace_name) do
    Path.join(System.tmp_dir!(), trace_name)
  end
end
