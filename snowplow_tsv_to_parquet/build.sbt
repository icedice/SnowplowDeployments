lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "snowplow-tsv-to-parquet",
    scalaVersion := "2.12.5",
    version      := "1.0-SNAPSHOT"
  )

mainClass in Compile := Some("dk.jp.snowplow_tsv_to_parquet.Main")

val parquetVersion = "1.10.0"
val hadoopVersion = "3.1.0"
val awsVersion = "1.11.467"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",

  "org.apache.parquet" % "parquet-common" % parquetVersion,
  "org.apache.parquet" % "parquet-encoding" % parquetVersion,
  "org.apache.parquet" % "parquet-column" % parquetVersion,
  "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
  "org.apache.parquet" % "parquet-avro" % parquetVersion,

  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  // The AWS SDK bundle from hadoop-aws takes up a lot of space so exclude that and set an explicit requirement for
  // the SDKs that are needed by hadoop-aws.
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion exclude("com.amazonaws", "aws-java-sdk-bundle"),
  "com.amazonaws" % "aws-java-sdk-dynamodb" % awsVersion, // For some reason, hadoop-aws also requires the DynamoDB SDK.
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion, // Our code requires the S3 SDK...
  "com.amazonaws" % "aws-java-sdk-sts" % awsVersion, // Local running of this code requires STS
  "com.amazonaws" % "aws-java-sdk-athena" % awsVersion, // ... and the Athena SDK.
  "org.json4s" %% "json4s-native" % "3.5.4"
)

dockerBaseImage := "openjdk:8-jre-slim"
