plugins {
    alias(libs.plugins.kotlin.jvm)
    id("maven-publish")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "ChronoGraph"
        )
    }
}

val sourceJar by tasks.registering(Jar::class) {
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
}

afterEvaluate {

    if(project.hasProperty("s3Url")){
        publishing {

            publications {
                create<MavenPublication>("mavenJava") {
                    from(components["java"])
                    artifact(sourceJar.get())

                    groupId = project.group.toString()
                    artifactId = project.name
                    version = project.property("mavenVersion") as String
                }
            }

            repositories {
                maven {
                    setUrl(project.property("s3Url") as String)
                    credentials(AwsCredentials::class) {
                        accessKey = project.property("s3AccessKey") as? String
                        secretKey = project.property("s3SecretKey") as? String
                    }
                }
            }

        }
    }


}

tasks.test {
    useJUnit {
        excludeCategories("org.chronos.common.test.junit.categories.PerformanceTest")
    }

    jvmArgs(
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
    )

    filter {
        includeTestsMatching("org.chronos.chronograph.test.cases.*")
        includeTestsMatching("org.chronos.chronograph.test._gremlinsuite.*")
    }

}


val chronoDbApiTestSources = project(":org.chronos.chronodb.api").extensions
    .getByType<JavaPluginExtension>()
    .sourceSets
    .getByName("test")
    .output

dependencies {
    api(project(":org.chronos.common"))
    api(project(":org.chronos.chronodb.api"))
    testImplementation(project(":org.chronos.common.testing"))
    testImplementation(project(":org.chronos.common"))
    testImplementation(chronoDbApiTestSources)

    // Backend-specific modules for unified tests
    testImplementation(project(":org.chronos.chronodb.exodus"))

    api(libs.gremlin.core) {
        // exclude transitive apache-commons libraries, we include them ourselves to have control over the versions.
        exclude(group = "commons-io")
        exclude(group = "org.apache.commons", module = "commons-text")
        exclude(group = "org.apache.commons", module = "commons-lang3")
        exclude(group = "org.apache.commons", module = "commons-configuration2")
        exclude(group = "commons-beanutils", module = "commons-beanutils")

    }
    api(libs.jakarta.annotations)

    implementation(libs.groovy)
    implementation(libs.kotlin.logging.jvm)
    implementation(libs.guava)
    implementation(libs.apache.commons.io)
    implementation(libs.apache.commons.lang3)
    implementation(libs.apache.commons.beanutils)
    api(libs.apache.commons.configuration2)

    testImplementation(libs.gremlin.test) {
        // exclude transitive apache-commons libraries, we include them ourselves to have control over the versions.
        exclude(group = "commons-io")
        exclude(group = "org.apache.commons", module = "commons-text")
        exclude(group = "org.apache.commons", module = "commons-lang3")
        exclude(group = "org.apache.commons", module = "commons-configuration2")
        exclude(group = "commons-beanutils", module = "commons-beanutils")
    }
    testImplementation(libs.bundles.testing)
}
