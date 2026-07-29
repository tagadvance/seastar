plugins {
    id("seastar.java-conventions")
    id("seastar.benchmark-conventions")
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

// Benchmarks live in their own source set: never on the default build, never in the published jar.
// The probe measures what the wire costs on top of an in-process session, so it needs a real driver
// and a real socket and nothing else.
val wireBench: SourceSet by sourceSets.creating

configurations["wireBenchImplementation"].extendsFrom(configurations["implementation"])
configurations["wireBenchRuntimeOnly"].extendsFrom(configurations["runtimeOnly"])

// ColdJvmBenchmark, the fork-a-fresh-JVM-per-sample harness, plus the Metrics format a probe prints.
// Shared from :seastar rather than reimplemented, so a startup number measured here is comparable
// with the in-process one measured there.
val benchHarnessSource = configurations.dependencyScope("benchHarnessSource")
val benchHarness = configurations.resolvable("benchHarness") {
    extendsFrom(benchHarnessSource.get())
}

dependencies {
    add("benchHarnessSource", project(path = ":seastar", configuration = "benchHarness"))
}

wireBench.compileClasspath += sourceSets["main"].output + benchHarness.get()
wireBench.runtimeClasspath += sourceSets["main"].output + benchHarness.get()

// A fresh JVM per sample rather than JMH, because class loading - Netty's, the driver's, and
// cassandra-all's parser - is most of what a startup number is made of, and JMH's warmup would
// erase exactly the thing under test.
tasks.register<JavaExec>("wireStartupBenchmark") {
    description = "Cold and warm startup of a driver over seastar-server, one fresh JVM per sample."
    group = "benchmark"
    mainClass = "com.tagadvance.seastar.bench.ColdJvmBenchmark"
    classpath = wireBench.runtimeClasspath
    args("com.tagadvance.seastar.bench.WireStartupProbe", "20")
    usesService(gradle.sharedServices.registrations["benchmarkExclusivity"].service)
}
