plugins {
    kotlin("jvm") version "1.9.20"
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