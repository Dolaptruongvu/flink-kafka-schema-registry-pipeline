name := "flink-kafka-schema-consumer"
version := "0.1"
scalaVersion := "2.12.15"

// Định nghĩa đúng artifact của Flink
libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-streaming-scala_2.12" % "1.14.6",
  "org.apache.flink" % "flink-scala_2.12" % "1.14.6",
  "org.apache.flink" % "flink-connector-kafka_2.12" % "1.14.6",
  "io.confluent" % "kafka-json-schema-serializer" % "7.2.0",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "org.apache.flink" % "flink-clients_2.12" % "1.14.6",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.everit.json" % "org.everit.json.schema" % "1.5.1"
)

// Thêm Confluent repository
resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
