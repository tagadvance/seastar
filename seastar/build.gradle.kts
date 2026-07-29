plugins {
    id("seastar.java-conventions")
    `java-test-fixtures`
    id("me.champeau.jmh") version "0.7.3"
}

description = "An in-memory implementation of the DataStax Java driver's CqlSession that mirrors " +
    "Cassandra's behavior, intended as a fast alternative to TestContainers for tests."

val logbackConfiguration = layout.projectDirectory.file("logback-tools.xml").asFile.absolutePath

// Versions are pinned rather than dynamic: the handlers reflect into package-private
// cassandra-all fields, so an unannounced upgrade can break the build with no code change.
dependencies {
    api("org.slf4j:slf4j-api:2.0.17")
    api("org.jspecify:jspecify:1.0.0")
    // Annotations are documentation only; nothing reads them reflectively at runtime.
    compileOnly("net.jcip:jcip-annotations:1.0")
    // cassandra-all is designed for running a Cassandra server/node
    implementation("org.apache.cassandra:cassandra-all:5.0.8")
    implementation("org.apache.cassandra:java-driver-core:4.19.3")
    implementation("org.apache.cassandra:java-driver-query-builder:4.19.3")

    // src/testFixtures holds AbstractCqlSessionTest - the fidelity suite - so that a backend in
    // another module can run it. It speaks only the driver API and JUnit, and both are part of the
    // fixture's own surface rather than an implementation detail, hence api rather than
    // implementation. java-driver-core is `implementation` above, so it does not arrive on its own.
    testFixturesApi("org.apache.cassandra:java-driver-core:4.19.3")
    testFixturesApi("org.junit.jupiter:junit-jupiter-api:6.1.2")
}

// The fixture is for this build's own modules. Publishing it would promise support for a suite that
// changes whenever SeaStar's expectations do.
val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            dependencies {
                implementation("org.mockito:mockito-core:5.23.0")
                implementation("org.mockito:mockito-junit-jupiter:5.23.0")
                implementation("org.testcontainers:testcontainers:2.0.5")
                implementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
                implementation("org.testcontainers:testcontainers-cassandra:2.0.5")
            }
        }
    }
}

// The container suite needs Docker, so it is opt-in: `./gradlew build` must be green on a
// machine that has none.
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("container")
    }
}

tasks.register<Test>("containerTest") {
    description = "Runs the fidelity suite against a real Cassandra node. Requires Docker."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("container")
    }
}

// Benchmarks live in their own source sets: never in the published jar, never on the default build.
//
// The container comparison is kept out of the jmh source set on purpose. TestContainers drags in the
// 3.x DataStax driver, whose Guava 19 and slf4j 1.7 end up ahead of the pinned versions in the JMH
// uber jar and break cassandra-all's static initializers at runtime.
val containerBench: SourceSet by sourceSets.creating

configurations["containerBenchImplementation"].extendsFrom(configurations["implementation"])
configurations["containerBenchRuntimeOnly"].extendsFrom(configurations["runtimeOnly"])

containerBench.compileClasspath += sourceSets["main"].output + sourceSets["jmh"].output
containerBench.runtimeClasspath += sourceSets["main"].output + sourceSets["jmh"].output

dependencies {
    "containerBenchImplementation"("org.testcontainers:testcontainers:2.0.5")
    "containerBenchImplementation"("org.testcontainers:testcontainers-cassandra:2.0.5")
}

jmh {
    // Benchmarks measure the library, not the test suite; keep the test source set out of them.
    includeTests = false
    // A quiet, deterministic logging configuration, so console I/O is not part of the measurement.
    jvmArgs = listOf("-Dlogback.configurationFile=" + logbackConfiguration)
    resultFormat = "TEXT"
    (project.findProperty("jmhIncludes") as String?)?.let { includes = it.split(",") }
}

// gradle.properties turns on parallel execution, which would let two benchmark tasks share the CPU
// and quietly ruin both. This service serializes them however they are invoked.
abstract class BenchmarkExclusivity : BuildService<BuildServiceParameters.None>

val benchmarkExclusivity =
    gradle.sharedServices.registerIfAbsent("benchmarkExclusivity", BenchmarkExclusivity::class) {
        maxParallelUsages = 1
    }

tasks.named<me.champeau.jmh.JMHTask>("jmh") {
    usesService(benchmarkExclusivity)
}

/**
 * Cold-JVM harnesses. These fork a fresh JVM per sample rather than using JMH steady-state
 * measurement, because class loading is most of what a startup number is made of.
 */
fun registerColdJvmBenchmark(name: String, probe: String, samples: Int, probeArgs: List<String>,
    sourceSet: SourceSet = sourceSets["jmh"]) =
    tasks.register<JavaExec>(name) {
        group = "benchmark"
        mainClass = "com.tagadvance.seastar.bench.ColdJvmBenchmark"
        classpath = sourceSet.runtimeClasspath
        systemProperty("logback.configurationFile", logbackConfiguration)
        args(listOf(probe, samples.toString()) + probeArgs)
        usesService(benchmarkExclusivity)
    }

registerColdJvmBenchmark("startupBenchmark", "com.tagadvance.seastar.bench.StartupProbe", 20,
    listOf("plain/clinitFirst/parseFirst")).configure {
    description = "Cold and warm SeaStarCqlSession startup, one fresh JVM per sample."
}

registerColdJvmBenchmark("startupSchemaBenchmark", "com.tagadvance.seastar.bench.StartupProbe", 20,
    listOf("schema")).configure {
    description = "Startup seeded with a realistic fixture schema via withSchema."
}

// The three variants are interleaved rather than run one after another: they are compared against
// each other, so a machine that throttles part way through must not favour whichever ran first.
registerColdJvmBenchmark("parserCostBenchmark", "com.tagadvance.seastar.bench.ParserCostProbe", 20,
    listOf("direct/queryProcessor/clinitOnly")).configure {
    description = "Attributes the one-time cassandra-all parser cost in a cold JVM."
}

registerColdJvmBenchmark("parserEquivalenceCheck", "com.tagadvance.seastar.bench.ParserCostProbe", 1,
    listOf("equivalence")).configure {
    description = "Checks both parser entry points return the same parse tree type."
}

listOf("warm" to 3, "cold" to 1).forEach { (mode, samples) ->
    registerColdJvmBenchmark("container${mode.replaceFirstChar(Char::uppercase)}Benchmark",
        "com.tagadvance.seastar.bench.ContainerProbe", samples, listOf(mode),
        containerBench).configure {
        description = "TestContainers Cassandra start to first query, $mode image. Requires Docker."
    }
}

tasks.register<JavaExec>("inspectRaw") {
    description = "Parses a CQL query and prints its CQLStatement.Raw class and fields."
    group = "verification"
    mainClass = "com.tagadvance.seastar.tools.CqlRawInspector"
    classpath = sourceSets["main"].runtimeClasspath
    systemProperty("logback.configurationFile", logbackConfiguration)
    args((project.findProperty("query") as String?)?.let { listOf(it) } ?: emptyList<String>())
    notCompatibleWithConfigurationCache("reads -Pquery at execution time")
}
