-- Test schema loaded from the classpath by SeaStarCqlSessionBuilderTest.
CREATE KEYSPACE shop WITH replication =
	{'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE shop.products (
	id int PRIMARY KEY,
	name text
);
