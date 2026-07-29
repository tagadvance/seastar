// Benchmarks live in their own source sets, in both modules: never on the default build, never in a
// published jar. gradle.properties turns on parallel execution, which would let two benchmark tasks
// share a CPU and quietly ruin both, so every benchmark task is serialized against every other by a
// shared service.
//
// The service is registered here rather than in a module's own build file because a shared service
// is keyed by name across the whole build: two modules each declaring their own
// `abstract class BenchmarkExclusivity` would be two different types under one name, which fails.
// Registering it once in a precompiled script plugin makes it one type, whoever applies it.
//
// A module uses it with:
//     usesService(gradle.sharedServices.registrations["benchmarkExclusivity"].service)

abstract class BenchmarkExclusivity : BuildService<BuildServiceParameters.None>

gradle.sharedServices.registerIfAbsent("benchmarkExclusivity", BenchmarkExclusivity::class) {
    maxParallelUsages = 1
}
