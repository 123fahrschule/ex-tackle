defmodule Tackle.Consumer.ExecutorTest do
  use ExUnit.Case
  alias Tackle.Consumer.Executor, as: TestConsumer

  describe "delivery" do
    test "consume pass" do
      assert :ok ==
               TestConsumer.delivery_handler(
                 fn -> :ok end,
                 fn _a -> :error end
               )
    end

    test "consume generates arithmetic exception" do
      bad_function = fn ->
        Code.eval_quoted(quote do: 1 / 0)
      end

      assert {:badarith, _stacktrace} =
               TestConsumer.delivery_handler(bad_function, fn reason -> reason end)
    end

    test "consume raises" do
      assert {%RuntimeError{message: "foo"}, _stacktrace} =
               TestConsumer.delivery_handler(fn -> raise "foo" end, fn reason -> reason end)
    end

    test "consume throws" do
      assert {{:nocatch, {:error, 12}}, _stacktrace} =
               TestConsumer.delivery_handler(fn -> throw({:error, 12}) end, fn reason ->
                 reason
               end)
    end

    test "consume signals" do
      assert :foo ==
               TestConsumer.delivery_handler(fn -> Process.exit(self(), :foo) end, fn reason ->
                 reason
               end)
    end

    test "can enforce retry with an throw(:retry)" do
      assert :retry_requested ==
               TestConsumer.delivery_handler(fn -> throw(:retry) end, fn {reason, _stacktrace} ->
                 reason
               end)
    end

    test "can enforce retry with an throw(:retry, reason)" do
      assert {:error, :error_reason} ==
               TestConsumer.delivery_handler(
                 fn -> throw({:retry, {:error, :error_reason}}) end,
                 fn {reason, _stacktrace} ->
                   reason
                 end
               )
    end
  end
end
