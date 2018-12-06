# Snowplow TSV to Parquet
Convert Snowplow TSV files to Parquet format while also extracting some information from the custom contexts to their own columns for faster lookup.

# Environment variables
```bash
IN_BUCKET=jpmedier-datalake-dev
OUT_BUCKET=behavior-datalake-dev
PARTITION_DATABASE=snowplow-dev
ATHENA_OUTPUT_LOCATION=aws-athena-query-results-092102721606-eu-west-1
```

# Compiling in IntelliJ IDEA
The default settings for the Scala Compile Server in IntelliJ IDEA will result in compilation errors due to the compiler crashing when type checking the code. To avoid this, change the `Xss` JVM option for the Scala Compile Server to `2m`. Go to `Settings` (`CTRL+ALT+S`) and change the `JVM options` setting for the Scala Compile Server:

![Scala Compile Server Settings](readme_intellij_scala_compile_server_settings.png "Scala Compile Server Settings")

# Backfilling using IntelliJ IDEA
If backfilling of snowplow-tsv-to-parquet is needed, following should be done:

1. Add Role informations in code (see description below)
2. Run `Backfill.scala` (dates for backfilling can be set in the code) with the previous mentioned `Environment variables` and `AWS_PROFILE=mfa` or lookup on `AWS ECS` for `Prod` variables.
  
In `/SnowplowDeployments/snowplow_tsv_to_parquet/src/main/scala/dk/jp/snowplow_tsv_to_parquet/athena/PartitionCatalog.scala`,
add following:

```bash
object PartitionCatalog {

val assumeRoleProvider = new STSAssumeRoleSessionCredentialsProvider.Builder("roleArn", "roleSessionName")
.build()

lazy val client: AmazonAthena = AmazonAthenaClientBuilder.standard().withCredentials(assumeRoleProvider).build()
...
```

In `/SnowplowDeployments/snowplow_tsv_to_parquet/src/main/scala/dk/jp/snowplow_tsv_to_parquet/Main.scala`,
add `assumeRoleProvider` and insert in `private implicit val s3`
```bash
object Main {
...
val assumeRoleProvider = new STSAssumeRoleSessionCredentialsProvider.Builder("roleArn", "roleSessionName")
.build()

  private implicit val s3: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withClientConfiguration(s3ClientConfig)
    .withCredentials(assumeRoleProvider)
    .withRegion("eu-west-1")
    .build()
```

In `build.sbt`:
```bash
"com.amazonaws" % "aws-java-sdk-sts" % "1.11.457"
```