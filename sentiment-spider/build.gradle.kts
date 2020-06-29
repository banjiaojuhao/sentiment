import cn.banjiaojuhao.sentiment.gradle.Versions
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile

plugins {
    id("org.jetbrains.kotlin.jvm") apply true
    application
}

dependencies {
    // Align versions of all Kotlin components
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", Versions.kotlinCoroutines)
    implementation("org.jetbrains.kotlin", "kotlin-reflect", Versions.kotlin)
    implementation("org.jetbrains.exposed", "exposed", Versions.exposed)

    // data extract
    implementation("org.jsoup", "jsoup", Versions.jsoup)
    implementation("com.alibaba", "fastjson", Versions.fastJson)
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.10.+")

    // data persistence
    implementation("org.mapdb", "mapdb", Versions.mapdb)
    implementation("com.uchuhimo", "konf", Versions.konf)
    implementation("org.xerial", "sqlite-jdbc", Versions.sqlite)
    implementation("mysql", "mysql-connector-java", Versions.mysql)

    implementation("io.netty", "netty-transport-native-epoll", Versions.epoll)
    implementation("io.vertx", "vertx-core", Versions.vertx)
    implementation("io.vertx", "vertx-web", Versions.vertx)
    implementation("io.vertx", "vertx-web-client", Versions.vertx)
    implementation("io.vertx", "vertx-lang-kotlin", Versions.vertx)
    implementation("io.vertx", "vertx-lang-kotlin-coroutines", Versions.vertx)


    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

application {
    mainClassName = "cn.banjiaojuhao.sentiment.crawler.AppKt"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        apiVersion = "1.3"
        languageVersion = "1.3"
    }
}
