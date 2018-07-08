defmodule Integration.EqualWorker do
  def perform(arg1, arg2) do
    if to_string(arg1) != arg2, do: throw "Different numbers"
    IO.puts "oa"
  end
end
