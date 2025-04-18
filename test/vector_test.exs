defmodule VectorTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  Code.require_file("support/vector_types.ex", __DIR__)

  setup_all do
    # Ensure pgvector extension is installed before starting connections
    PSQL.cmd(["-d", System.get_env("PGDATABASE", "postgrex_test"), "-c", "CREATE EXTENSION IF NOT EXISTS vector"])
    :ok
  end

  @moduletag :vector

  setup do
    # Use environment variables for database connection details
    opts = [
      hostname: System.get_env("PGHOST", "localhost"),
      username: System.get_env("PGUSER", "postgres"),
      password: System.get_env("PGPASSWORD", "postgres"),
      database: System.get_env("PGDATABASE", "postgrex_test"),
      backoff_type: :stop,
      # Use our custom types module to ensure vector extension is loaded
      types: VectorTest.Types
    ]

    {:ok, pid} = Postgrex.start_link(opts)
    {:ok, [pid: pid]}
  end

  defp extension_installed?(pid) do
    result =
      Postgrex.query!(
        pid,
        "SELECT COUNT(*) FROM pg_available_extensions WHERE name = 'vector'",
        []
      )

    case result.rows do
      [[1]] -> true
      _ -> false
    end
  end

  defp skip_if_no_extension(pid) do
    unless extension_installed?(pid) do
      flunk("pgvector extension is not available")
    end
  end

  test "encode and decode vector", context do
    # Table will use existing extension
    skip_if_no_extension(context.pid)

    # Create vector table
    :ok =
      query("CREATE TABLE IF NOT EXISTS vector_test (id serial PRIMARY KEY, vec vector(3))", [])

    :ok = query("TRUNCATE vector_test", [])

    # Insert a vector using a direct list of floats
    vector_data = [1.0, 2.0, 3.0]
    :ok = query("INSERT INTO vector_test (vec) VALUES ($1)", [vector_data])

    # Retrieve the vector from the database
    result = query("SELECT vec FROM vector_test", [])
    [retrieved_vector] = hd(result)

    # Verify the retrieved vector matches the original
    assert is_list(retrieved_vector)
    assert length(retrieved_vector) == 3
    assert retrieved_vector == vector_data

    # Clean up
    :ok = query("DROP TABLE vector_test", [])
  end

  test "direct vector parameters with cosine similarity search", context do
    skip_if_no_extension(context.pid)

    # Create the test table
    :ok = query("DROP TABLE IF EXISTS vector_direct_test", [])

    :ok =
      query(
        "CREATE TABLE vector_direct_test (id serial PRIMARY KEY, embedding vector(3), label text)",
        []
      )

    # Sample vectors for testing - directly as lists of floats
    test_vectors = [
      {[1.0, 0.0, 0.0], "pure x"},
      {[0.0, 1.0, 0.0], "pure y"},
      {[0.0, 0.0, 1.0], "pure z"},
      {[0.5, 0.3, 0.2], "mixed a"},
      {[0.2, 0.7, 0.1], "mixed b"},
      {[0.1, 0.1, 0.8], "mixed c"}
    ]

    # Insert vectors using direct parameter binding
    for {vector, label} <- test_vectors do
      sql = "INSERT INTO vector_direct_test (embedding, label) VALUES ($1, $2)"
      assert {:ok, _} = Postgrex.query(context.pid, sql, [vector, label])
    end

    # Verify insertion worked by retrieving the vectors
    {:ok, result} =
      Postgrex.query(
        context.pid,
        "SELECT embedding, label FROM vector_direct_test ORDER BY id",
        []
      )

    # Check that we got back the vector data and labels correctly
    for {i, {orig_vector, orig_label}} <- Enum.with_index(test_vectors) do
      [retrieved_vector, retrieved_label] = Enum.at(result.rows, i)

      # Check that the vector and label match
      assert retrieved_vector == orig_vector
      assert retrieved_label == orig_label
    end

    # Test cosine similarity search - use parameterized vector
    search_vector = [0.1, 0.8, 0.1]

    {:ok, search_result} =
      Postgrex.query(
        context.pid,
        "SELECT label, embedding <=> $1 AS distance
         FROM vector_direct_test
         ORDER BY embedding <=> $1
         LIMIT 3",
        [search_vector]
      )

    # Extract results
    [[closest_label, closest_dist], [_second_label, second_dist], [_third_label, third_dist]] =
      search_result.rows

    # Verify "mixed b" is the closest match
    assert closest_label == "mixed b"

    # Check that distances are ordered correctly
    assert closest_dist < second_dist
    assert second_dist < third_dist

    # Clean up
    :ok = query("DROP TABLE IF EXISTS vector_direct_test", [])
  end

  test "vector operations with integer values converted to floats", context do
    skip_if_no_extension(context.pid)

    # Create the test table
    :ok = query("DROP TABLE IF EXISTS vector_int_test", [])
    :ok = query("CREATE TABLE vector_int_test (id serial PRIMARY KEY, embedding vector(3))", [])

    # Test with a mixture of integers and floats
    mixed_vector = [1, 2.5, 3]

    # Insert using direct parameter binding
    assert {:ok, _} =
             Postgrex.query(
               context.pid,
               "INSERT INTO vector_int_test (embedding) VALUES ($1)",
               [mixed_vector]
             )

    # Retrieve and check
    {:ok, result} = Postgrex.query(context.pid, "SELECT embedding FROM vector_int_test", [])
    [[retrieved_vector]] = result.rows

    # Verify the values were correctly converted (integers become floats)
    assert is_list(retrieved_vector)
    assert length(retrieved_vector) == 3
    assert Enum.all?(retrieved_vector, &is_float/1)
    assert retrieved_vector == [1.0, 2.5, 3.0]

    # Clean up
    :ok = query("DROP TABLE IF EXISTS vector_int_test", [])
  end

  test "vector cosine similarity search with multiple vectors", context do
    skip_if_no_extension(context.pid)

    # Create the test table
    :ok = query("DROP TABLE IF EXISTS vector_search_test", [])

    :ok =
      query(
        "CREATE TABLE vector_search_test (id serial PRIMARY KEY, embedding vector(3), description text)",
        []
      )

    # Sample vectors for testing
    test_vectors = [
      {[1.0, 0.0, 0.0], "X axis unit vector"},
      {[0.0, 1.0, 0.0], "Y axis unit vector"},
      {[0.0, 0.0, 1.0], "Z axis unit vector"},
      {[0.5, 0.5, 0.5], "Diagonal vector (balanced)"},
      {[0.8, 0.3, 0.1], "Mostly X vector"},
      {[0.1, 0.1, 0.9], "Mostly Z vector"}
    ]

    # Insert vectors using parameter binding
    for {vector, description} <- test_vectors do
      sql = "INSERT INTO vector_search_test (embedding, description) VALUES ($1, $2)"
      assert {:ok, _} = Postgrex.query(context.pid, sql, [vector, description])
    end

    # Test 1: Find vectors most similar to [1,0,0] (should be X axis and Mostly X)
    search_vector_1 = [1.0, 0.0, 0.0]

    {:ok, result} =
      Postgrex.query(
        context.pid,
        "SELECT id, description, embedding <=> $1 AS distance
         FROM vector_search_test
         ORDER BY embedding <=> $1
         LIMIT 3",
        [search_vector_1]
      )

    [[_id1, desc1, dist1], [_id2, desc2, _dist2], [_id3, _desc3, _dist3]] = result.rows

    # The closest should be the X axis vector itself
    assert desc1 == "X axis unit vector"
    # Almost zero distance
    assert dist1 < 0.01

    # Second closest should be the "Mostly X vector"
    assert desc2 == "Mostly X vector"

    # Test 2: Find vectors most similar to [0,0,1] (should be Z axis and Mostly Z)
    search_vector_2 = [0.0, 0.0, 1.0]

    {:ok, result} =
      Postgrex.query(
        context.pid,
        "SELECT id, description, embedding <=> $1 AS distance
         FROM vector_search_test
         ORDER BY embedding <=> $1
         LIMIT 3",
        [search_vector_2]
      )

    [[_id1, desc1, dist1], [_id2, desc2, _dist2], [_id3, _desc3, _dist3]] = result.rows

    # The closest should be the Z axis vector itself
    assert desc1 == "Z axis unit vector"
    # Almost zero distance
    assert dist1 < 0.01

    # Second closest should be the "Mostly Z vector"
    assert desc2 == "Mostly Z vector"

    # Clean up
    :ok = query("DROP TABLE IF EXISTS vector_search_test", [])
  end
end
