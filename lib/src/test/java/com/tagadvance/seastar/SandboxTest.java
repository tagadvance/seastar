package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

class SandboxTest {

	@Test
	void test() {
		final var session = CqlSession.builder()
			.addContactPoint(new InetSocketAddress("172.16.0.202", 9042))
			.withAuthCredentials("casandra", "cassandra")
			.withLocalDatacenter("datacenter1")
			.build();

		session.execute("""
			CREATE TYPE foo.phone_profile (
					country_code int,
					phone_number text
				);""");

//		session.execute(
//			"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
//		session.execute("USE foo");
//
//		var execute1 =
//
//		var execute2 = session.execute("""
//			CREATE TABLE xxx.users (
//			    user_id UUID PRIMARY KEY,
//			    first_name text
//			);""");

		System.gc();
	}

}
