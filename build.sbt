// Common settings
val defaultScalaVersion = "2.13.3"
organization in ThisBuild := "com.davideicardi"
scalaVersion in ThisBuild := defaultScalaVersion
scalacOptions in ThisBuild ++= Seq(
    "-language:higherKinds", // Allow higher kinds types (for scala 2.12 only)
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-explaintypes", // Explain type errors in more detail.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:package-object-classes", // Class or object defined in package object.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates", // Warn if a private member is unused.
    "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
)

val avro4sVersion = "4.0.0"
val kafkaVersion = "2.6.0"
val scalaTestVersion = "3.2.2"
val AkkaVersion = "2.6.8"
val AkkaHttpVersion = "10.2.1"

def testDependencies(scope: String) = Seq(
    "org.scalamock" %% "scalamock" % "4.4.0" % scope,
    "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % scope,
    "org.scalatest" %% "scalatest-funspec" % scalaTestVersion % scope,
    "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % scope,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % scope,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion % scope,
    "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion % scope,
)

lazy val es4kafka = (project in file("es4kafka"))
        .settings(
            name := "es4kafka",
            libraryDependencies ++= Seq(
                // kafka streams
                "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
                "org.apache.kafka" % "kafka-clients" % kafkaVersion,
                // akka
                "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
                // rest api
                "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
                "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
                "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
                // rest api kafka producer
                "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.5"
                        exclude("com.typesafe.akka", "akka-stream_2.13")
                        exclude("org.apache.kafka", "kafka-clients"),
                // avro schema registry
                "com.davideicardi" %% "kaa" % "0.4.5"
                        exclude("org.apache.kafka", "kafka-clients"),
                // logging
                "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
                "ch.qos.logback" % "logback-classic" % "1.2.3"
                        exclude("org.slf4j", "slf4j-api"),
                // retry library
                "com.softwaremill.retry" %% "retry" % "0.3.3",
                // guice
                "net.codingwell" %% "scala-guice" % "4.2.11",
            ),
            libraryDependencies ++= testDependencies("test"),
        )

lazy val es4kafkaTest = (project in file("es4kafka-test"))
        .settings(
            name := "es4kafka-test",
            libraryDependencies ++= testDependencies("compile"),
        )
        .dependsOn(es4kafka)

lazy val sample_books_catalog = (project in file("examples/books-catalog"))
        .configs(IntegrationTest)
        .settings(
            name := "sample-books-catalog",
            Defaults.itSettings,
            publish / skip := true,
            libraryDependencies ++= testDependencies("it, test"),
        )
        .dependsOn(es4kafka, es4kafkaTest % "it, test")

lazy val sample_bank_account = (project in file("examples/bank-account"))
        .configs(IntegrationTest)
        .settings(
            name := "sample-bank-account",
            Defaults.itSettings,
            publish / skip := true,
            libraryDependencies ++= testDependencies("it, test"),
        )
        .dependsOn(es4kafka, es4kafkaTest % "it, test")

lazy val es4kafka_root = (project in file("."))
        .aggregate(es4kafka, sample_books_catalog, sample_bank_account, es4kafkaTest)
