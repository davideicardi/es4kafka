val avro4sVersion = "4.0.0"
val kafkaVersion = "2.4.0"
val scalaTestVersion = "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "sample-stream-app",
    scalaVersion := "2.13.3",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "com.davideicardi" %% "kaa" % "0.3.3",
      "org.scalatest" %% "scalatest-funspec"       % scalaTestVersion % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka-streams" % kafkaVersion % Test
    )
  )