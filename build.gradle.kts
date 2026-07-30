// The root project builds nothing. This file exists for one reason: it loads the convention
// plugin's classpath - and with it com.vanniktech.maven.publish - into the root project's
// classloader scope, which every subproject inherits.
//
// Without it, :seastar and :seastar-server each load the publish plugin in their own scope, and the
// Central Portal build service that plugin registers is then two different types under one name.
// Gradle fails with "Cannot set the value of task ':seastar-server:prepareMavenCentralPublishing'
// property 'buildService' ... loaded with InstrumentingVisitableURLClassLoader(... project-seastar
// ...)" and suggests exactly this fix. `apply false`, because the root project is not published.
plugins {
    id("seastar.java-conventions") apply false
}
