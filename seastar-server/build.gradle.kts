plugins {
    id("seastar.java-conventions")
}

description = "A native-protocol listener that serves an in-memory SeaStar CqlSession over the " +
    "wire, so a driver that cannot be replaced in-process can still talk to it."

// Pinned exactly, like the core's dependencies and for a sharper reason: this module compiles
// against com.datastax.oss.protocol.internal.* and com.datastax.oss.driver.internal.core.protocol.*,
// both of which are explicitly unstable. A minor bump can move them with no deprecation cycle.
//
// netty-handler rather than netty-all: the core drags netty-all in transitively through
// cassandra-all, which is an accident of the parser dependency rather than a decision.
//
// native-protocol arrives transitively via java-driver-core and is already on the core's runtime
// classpath. It is declared anyway - the whole module is built against it, and an implicit
// transitive is not a contract.
dependencies {
    implementation(project(":seastar"))
    implementation("io.netty:netty-handler:4.1.130.Final")
    implementation("com.datastax.oss:native-protocol:1.5.2")
}

// javadoc fails outright on a package holding nothing but package-info.java. Delete this once the
// listener has its first public type.
tasks.named<Javadoc>("javadoc") {
    onlyIf { task -> (task as Javadoc).source.any { it.name != "package-info.java" } }
}
