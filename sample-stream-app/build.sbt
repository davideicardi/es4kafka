lazy val root = (project in file("."))
  .settings(
    name := "sample-stream-app",
    scalaVersion := "2.13.3",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka-streams-scala" % "2.6.0",
      "org.scalatest" %% "scalatest-wordspec"       % "3.2.2" % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.2" % Test,      
      "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0" % Test,
      "io.github.embeddedkafka" %% "embedded-kafka-streams" % "2.6.0" % Test
    )
  )