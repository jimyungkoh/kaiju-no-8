plugins {
    kotlin("jvm") version "2.0.0"
}

allprojects{
    group = "org.example"
    version = "1.0-SNAPSHOT"
    repositories {
        mavenCentral()
    }
}

subprojects{
    apply(plugin = "org.jetbrains.kotlin.jvm")
}