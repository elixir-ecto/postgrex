defmodule TsvectorTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P
  alias Postgrex.Lexeme

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "encode basic tsvector", context do
    assert [["'1'"]] = query("SELECT $1::tsvector::text", [[%Lexeme{positions: [], word: "1"}]])

    assert [["'1' 'hello'"]] =
             query("SELECT $1::tsvector::text", [
               [%Lexeme{positions: [], word: "1"}, %Lexeme{positions: [], word: "hello"}]
             ])
  end

  test "encode tsvector with positions", context do
    assert [["'1':1"]] =
             query("SELECT $1::tsvector::text", [[%Lexeme{positions: [{1, nil}], word: "1"}]])
  end

  test "encode tsvector with multiple positions", context do
    assert [["'1':1,2"]] =
             query("SELECT $1::tsvector::text", [
               [%Lexeme{positions: [{1, nil}, {2, nil}], word: "1"}]
             ])
  end

  test "encode tsvector with position and weight", context do
    assert [["'car':1A"]] =
             query("SELECT $1::tsvector::text", [[%Lexeme{positions: [{1, :A}], word: "car"}]])

    assert [["'car':1B"]] =
             query("SELECT $1::tsvector::text", [[%Lexeme{positions: [{1, :B}], word: "car"}]])

    assert [["'car':1C"]] =
             query("SELECT $1::tsvector::text", [[%Lexeme{positions: [{1, :C}], word: "car"}]])
  end

  test "encode tsvector with multiple positions and weights", context do
    assert [["'car':1A,2,3B"]] =
             query("SELECT $1::tsvector::text", [
               [%Lexeme{positions: [{1, :A}, {2, nil}, {3, :B}], word: "car"}]
             ])
  end

  test "decode basic tsvectors", context do
    assert [[[%Lexeme{positions: [], word: "1"}]]] = query("SELECT '1'::tsvector", [])
    assert [[[%Lexeme{positions: [], word: "1"}]]] = query("SELECT '1 '::tsvector", [])
    assert [[[%Lexeme{positions: [], word: "1"}]]] = query("SELECT ' 1'::tsvector", [])
    assert [[[%Lexeme{positions: [], word: "1"}]]] = query("SELECT ' 1 '::tsvector", [])
  end

  test "decode tsvectors with multiple elements", context do
    assert [[[%Lexeme{positions: [], word: "1"}, %Lexeme{positions: [], word: "2"}]]] =
             query("SELECT '1 2'::tsvector", [])

    assert [[[%Lexeme{positions: [], word: "1 2"}]]] = query("SELECT '''1 2'''::tsvector", [])
  end

  test "decode tsvectors with multiple positions and elements", context do
    assert [
             [
               [
                 %Lexeme{positions: [{8, nil}], word: "a"},
                 %Lexeme{positions: [{1, nil}, {2, :C}, {3, :B}, {4, :A}, {5, nil}], word: "w"}
               ]
             ]
           ] = query("SELECT '''w'':4A,2C,3B,1D,5 a:8'::tsvector", [])

    assert [
             [
               [
                 %Lexeme{positions: [{3, :A}], word: "a"},
                 %Lexeme{positions: [{2, :A}], word: "b"}
               ]
             ]
           ] = query("SELECT 'a:3A b:2a'::tsvector", [])
  end
end
