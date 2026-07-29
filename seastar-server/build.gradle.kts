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
// java-driver-core is `implementation` in :seastar rather than `api`, so it does not reach this
// module's compile classpath on its own. It is declared here instead of being promoted there:
// b_plan's brief is a listener, not a change to what every consumer of the core gets, and a_plan A7
// asks for the core's published surface to be protected.
dependencies {
    implementation(project(":seastar"))
    implementation("org.apache.cassandra:java-driver-core:4.19.3")
    implementation("io.netty:netty-handler:4.1.130.Final")
    implementation("com.datastax.oss:native-protocol:1.5.2")
    // Annotations are documentation only; nothing reads them reflectively at runtime.
    compileOnly("net.jcip:jcip-annotations:1.0")
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            dependencies {
                implementation(testFixtures(project(":seastar")))
            }
        }
    }
}
