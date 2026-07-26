plugins {
    `java-library`
    `maven-publish`
    signing
}

base {
    archivesName = "seastar"
}

repositories {
    mavenCentral()
}

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
    implementation("com.google.guava:guava:33.5.0-jre")
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter("6.1.2")
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

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.register<JavaExec>("inspectRaw") {
    description = "Parses a CQL query and prints its CQLStatement.Raw class and fields."
    group = "verification"
    mainClass = "com.tagadvance.seastar.tools.CqlRawInspector"
    classpath = sourceSets["main"].runtimeClasspath
    systemProperty("logback.configurationFile", layout.projectDirectory.file("logback-tools.xml").asFile.absolutePath)
    args((project.findProperty("query") as String?)?.let { listOf(it) } ?: emptyList<String>())
    notCompatibleWithConfigurationCache("reads -Pquery at execution time")
}

group = "com.tagadvance"
version = "1.0.0-SNAPSHOT"

java {
    withSourcesJar()
    withJavadocJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "seastar"
            from(components["java"])

            pom {
                name.set("SeaStar")
                description.set("...")
                url.set("https://github.com/tagadvance/seastar")


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
            }
        }
    }

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

signing {
    val signingKey = System.getenv("GPG_SIGNING_KEY")
    val signingPassword = System.getenv("GPG_SIGNING_PASSWORD")
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications)
}
