plugins {
    java
    `java-library`
}

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:${property("jacksonVersion")}")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${property("jacksonVersion")}")
    api("jakarta.validation:jakarta.validation-api:3.0.2")

    compileOnly("org.projectlombok:lombok:${property("lombokVersion")}")
    annotationProcessor("org.projectlombok:lombok:${property("lombokVersion")}")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.assertj:assertj-core:3.25.3")
}
