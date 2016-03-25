goal
====

compare stream, execute

    c( "benchmarks/csv.ex" )
	  {:ok, pid} = Postgrex.start_link(hostname: "localhost", username: "queen", password: "honey", database: "hive2" )
		{:ok, q} = Postgrex.prepare( pid, "", "select * from products" )

stats
=====

		hive2=# select pg_size_pretty( pg_relation_size( oid ) ) from pg_class where relname='products';
		pg_size_pretty
		----------------
		72 MB


		hive2=# select count(*) from products;
		count
		--------
		275585

execute
=======

		Benchmark.execute( pid, q, log: &Benchmark.log/1 )                  
		execute 51 601472 2616991
		13155791

		executes = 1..10 |> Enum.map( fn _ -> Benchmark.execute( pid, q ) end )  
		[5106804, 5661593, 5276430, 5306967, 4394412, 5354833, 5538348, 4736963,
		4868619, 5452068]
		Enum.sum( executes ) / 10
		5169703.7

stream
======

		streams = 1..10 |> Enum.map( fn _ -> Benchmark.stream( pid, q, max_rows: 1000 ) end )
		[4750766, 4262353, 4088620, 4256172, 4101127, 4336613, 4103111, 4240365,
		4079095, 4282869]

		Enum.sum( streams ) / 10                                                             
		4250109.1

TaiL;DR
=======
+17% in lead time
