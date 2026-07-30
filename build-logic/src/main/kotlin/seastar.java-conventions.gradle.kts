import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SourcesJar
import org.gradle.jvm.toolchain.JavaToolchainService

// Everything both published modules share: toolchain, warnings, archive determinism, and the
// whole publishing block. a_plan A4 chose a convention plugin over `subprojects { }` because the
// latter fights the configuration cache that gradle.properties turns on.

plugins {
    `java-library`
    signing
    // Applies Gradle's own maven-publish, then targets the Sonatype Central Portal. `.base` rather
    // than the full `com.vanniktech.maven.publish`, because the full plugin decides what to do from
    // Gradle properties (SONATYPE_HOST, RELEASE_SIGNING_ENABLED, POM_*) and this build says it all
    // in one file instead.
    id("com.vanniktech.maven.publish.base")
}

// a_plan A5: both modules are versioned from here, in lockstep, always. seastar-server is built
// against the core's internals as much as against the driver's, so a mismatched pair is not a
// combination worth supporting.
//
// THE GROUPID IS THIS ONE LINE. If verifying `tagadvance.com` on the Central Portal cannot be
// resolved, change it to "io.github.tagadvance" - that namespace is verified by creating a GitHub
// repository of the name the portal gives you, rather than by a DNS record. A Maven groupId does not
// have to match the Java package: `io.github.tagadvance:seastar` shipping classes in
// `com.tagadvance.seastar` is legal and unremarkable, because Central only cares that the namespace
// is yours. So the fallback costs the coordinates in README.md and docs/, and no source change at
// all. Nothing has been published yet, so it is free now and stops being free at 1.0.0.
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

// What switches signing on, and the only thing that does. Keep it a condition: the plugin's
// signAllPublications() makes signing *required* for any version that is not a -SNAPSHOT, so
// calling it unconditionally would break publishToMavenLocal for a contributor with no GPG key -
// the one publishing path that has to work without credentials (j_plan J4).
val signingKey = providers.environmentVariable("GPG_SIGNING_KEY").orNull

// RELEASE IS STILL BLOCKED, but no longer by anything in this file. What is left is external, and
// only the account holder can do it:
//
//   1. verify the groupId's namespace at https://central.sonatype.com. Verifying `tagadvance.com`
//      hit a conflict; the fallback is `io.github.tagadvance`, see the `group` line above.
//   2. export a Portal token - not the old OSSRH login - under the names the plugin reads:
//        export ORG_GRADLE_PROJECT_mavenCentralUsername="$SONATYPE_USER"
//        export ORG_GRADLE_PROJECT_mavenCentralPassword="$SONATYPE_PASSWORD"
//      The plugin takes these as Gradle properties, and `ORG_GRADLE_PROJECT_` is how an environment
//      variable becomes one. SONATYPE_USER / SONATYPE_PASSWORD on their own are not read by
//      anything any more.
//   3. export GPG_SIGNING_KEY, and GPG_SIGNING_PASSWORD if the key has one.
//
// Then `./gradlew publishToMavenCentral`, and release the deployment by hand from the portal.
// publishToMavenLocal needs none of the three and must stay that way.
mavenPublishing {
    // 0.37.0 speaks to the Central Portal and nothing else - SonatypeHost is gone from the API, and
    // an OSSRH value now fails with a pointer to the sunset notice. A -SNAPSHOT version goes to
    // https://central.sonatype.com/repository/maven-snapshots/; a release is staged under
    // build/publishing/mavenCentral and uploaded as a single bundle at the end of the build. No
    // argument means no automatic release: the deployment sits in the portal until it is released
    // by hand, which is what we want for a first publish.
    publishToMavenCentral()

    // None()/None() because `java { withSourcesJar(); withJavadocJar() }` above already produces
    // both jars, and - the part that matters - the sourcesElements and javadocElements *variants*
    // the published .module carries. Letting the plugin add its own would be a second artifact
    // under the same classifier. None() for sources also stops the plugin adding a
    // testFixturesSourcesElements variant to :seastar, which a_plan A3 would then have to skip
    // alongside the two it already skips.
    configure(JavaLibrary(javadocJar = JavadocJar.None(), sourcesJar = SourcesJar.None()))

    if (!signingKey.isNullOrBlank()) {
        signAllPublications()
    }

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

// The plugin's own in-memory key comes from a `signingInMemoryKey` Gradle property. The key here has
// always been GPG_SIGNING_KEY, an environment variable, so it is handed to Gradle's signing
// extension directly and the plugin only has to know that publications are signed.
if (!signingKey.isNullOrBlank()) {
    signing {
        useInMemoryPgpKeys(signingKey, providers.environmentVariable("GPG_SIGNING_PASSWORD").orNull)
    }
}
