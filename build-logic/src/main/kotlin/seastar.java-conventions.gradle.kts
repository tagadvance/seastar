import org.gradle.jvm.toolchain.JavaToolchainService

// Everything both published modules share: toolchain, warnings, archive determinism, and the
// whole publishing block. a_plan A4 chose a convention plugin over `subprojects { }` because the
// latter fights the configuration cache that gradle.properties turns on.

plugins {
    `java-library`
    `maven-publish`
    signing
}

// a_plan A5: both modules are versioned from here, in lockstep, always. seastar-server is built
// against the core's internals as much as against the driver's, so a mismatched pair is not a
// combination worth supporting.
group = "com.tagadvance"
version = "1.0.0-alpha"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    withSourcesJar()
    withJavadocJar()
}

testing {
    suites {
        withType<JvmTestSuite>().configureEach {
            useJUnitJupiter("6.1.2")
        }
    }
}

// The published bytecode is always 17; this only changes the JVM the tests run on, so CI can prove
// the setAccessible reflection into cassandra-all still works on a newer runtime.
// ./gradlew test -PtestJavaVersion=21
val testJavaVersion = (project.findProperty("testJavaVersion") as String?)?.toInt()
if (testJavaVersion != null) {
    val toolchains = extensions.getByType<JavaToolchainService>()
    tasks.withType<Test>().configureEach {
        javaLauncher = toolchains.launcherFor {
            languageVersion = JavaLanguageVersion.of(testJavaVersion)
        }
    }
}

tasks.withType<JavaCompile>().configureEach {
    // Explicit here as well as in the toolchain so the bytecode target is visible in this file.
    options.release = 17
    options.compilerArgs.add("-Xlint:all")
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
}

tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

tasks.named<Jar>("jar") {
    // :seastar -> com.tagadvance.seastar, :seastar-server -> com.tagadvance.seastar.server.
    manifest {
        attributes("Automatic-Module-Name" to "com.tagadvance." + project.name.replace('-', '.'))
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = project.name
            from(components["java"])

            pom {
                name.set(project.name)
                description.set(provider { project.description })
                url.set("https://github.com/tagadvance/seastar")
                inceptionYear.set("2026")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://raw.githubusercontent.com/tagadvance/seastar/main/LICENSE")
                    }
                }

                organization {
                    name.set("tagadvance")
                    url.set("https://tagadvance.com")
                }

                developers {
                    developer {
                        id.set("tagadvance")
                        name.set("Tag Spilman")
                        email.set("tagadvance+SeaStar@gmail.com")
                        organization.set("tagadvance")
                        organizationUrl.set("https://tagadvance.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com:tagadvance/seastar.git")
                    developerConnection.set("scm:git:ssh://git@github.com:tagadvance/seastar.git")
                    url.set("https://github.com/tagadvance/seastar")
                }

                issueManagement {
                    system.set("GitHub Issues")
                    url.set("https://github.com/tagadvance/seastar/issues")
                }
            }
        }
    }

    // RELEASE IS BLOCKED HERE, and not by anything this build can fix.
    //
    // Both URLs below are dead. Sonatype retired OSSRH on 30 June 2025 and s01.oss.sonatype.org went
    // with it; the replacement is the Central Portal, which has a different publishing endpoint and
    // will not accept anything until the com.tagadvance namespace has been verified. That
    // verification is a manual, external, one-off step only the account holder can perform, and it
    // is the long pole. It now gates two artifacts rather than one.
    //
    // The dead host is left in place deliberately. A guessed replacement endpoint would fail with an
    // authentication error and read like a credentials problem; this fails as what it is.
    //
    // To unblock, in order:
    //   1. verify the com.tagadvance namespace at https://central.sonatype.com
    //   2. replace both repositories below with that portal's publishing endpoint
    //   3. set SONATYPE_USER / SONATYPE_PASSWORD to a portal token, not the old OSSRH login
    //   4. set GPG_SIGNING_KEY, which is what switches on the signing block at the end of this file
    //
    // None of this touches publishToMavenLocal, which works today, with or without a GPG key. Keep
    // it that way: it is the only publishing path a contributor without credentials can run.
    repositories {
        maven("https://s01.oss.sonatype.org/content/repositories/snapshots/") {
            name = "SonatypeSnapshot"
            credentials {
                username = System.getenv("SONATYPE_USER")
                password = System.getenv("SONATYPE_PASSWORD")
            }
        }
        maven("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/") {
            name = "SonatypeStaging"
            credentials {
                username = System.getenv("SONATYPE_USER")
                password = System.getenv("SONATYPE_PASSWORD")
            }
        }
    }
}

// Only configure signing when a key is actually present, so that every documented Gradle
// command - publishToMavenLocal included - works for a contributor with no credentials.
val signingKey = providers.environmentVariable("GPG_SIGNING_KEY").orNull

if (!signingKey.isNullOrBlank()) {
    signing {
        useInMemoryPgpKeys(signingKey, providers.environmentVariable("GPG_SIGNING_PASSWORD").orNull)
        sign(publishing.publications)
    }
}
