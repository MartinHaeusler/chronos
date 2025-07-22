plugins {
    alias(libs.plugins.kotlin.jvm)
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "Chronos Common Testing Utils"
        )
    }
}

dependencies {
    implementation(project(":org.chronos.common"))

    api(libs.hamcrest)
    api("junit:junit:4.13.2")
    implementation(libs.apache.commons.io)
    implementation(libs.slf4j.api)
    implementation(libs.guava)

    testImplementation(libs.bundles.testing)
}
