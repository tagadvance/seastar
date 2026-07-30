plugins {
    `kotlin-dsl`
}

// Pinned exactly, like every other version in this build. seastar.java-conventions applies
// com.vanniktech.maven.publish.base, and a precompiled script plugin can only apply a plugin that is
// on the compile classpath of the build defining it.
dependencies {
    implementation("com.vanniktech:gradle-maven-publish-plugin:0.37.0")
}
