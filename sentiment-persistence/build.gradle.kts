import cn.banjiaojuhao.sentiment.gradle.Versions
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile


plugins {
    id("org.jetbrains.kotlin.jvm") apply true
}

dependencies {
    // Align versions of all Kotlin components
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", Versions.kotlinCoroutines)
    implementation("org.jetbrains.kotlin", "kotlin-reflect", Versions.kotlin)
    implementation("org.jetbrains.exposed", "exposed", Versions.exposed)

    implementation("mysql", "mysql-connector-java", Versions.mysql)


    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
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
