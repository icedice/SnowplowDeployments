# Snowplow TSV to Parquet
Convert Snowplow TSV files to Parquet format while also extracting some information from the custom contexts to their own columns for faster lookup.

# Compiling in IntelliJ IDEA
The default settings for the Scala Compile Server in IntelliJ IDEA will result in compilation errors due to the compiler crashing when type checking the code. To avoid this, change the `Xss` JVM option for the Scala Compile Server to `2m`. Go to `Settings` (`CTRL+ALT+S`) and change the `JVM options` setting for the Scala Compile Server:

![Scala Compile Server Settings](readme_intellij_scala_compile_server_settings.png "Scala Compile Server Settings")

# Building and deploy JAR file
Make sure to update the version number in *build.sbt*.

```bash
VERSION="1.0.0"
CLOUD_INFRASTRUCTURE_FOLDER="..."

# Build the JAR file.
sbt assembly

# Copy the JAR file to the Terraform folder.
cp target/scala-2.12/snowplow_s3_to_parquet-assembly-${VERSION}.jar ${CLOUD_INFRASTRUCTURE_FOLDER}/Terraform/snowplow/snowplow_tsv_to_parquet/

# Make changes to the lambda_jar_version variable to match ${VERSION}.
cd ${CLOUD_INFRASTRUCTURE_FOLDER}/Terraform/snowplow/snowplow_tsv_to_parquet
??? # Syntax error so you don't forget it if you just copy blindly from this script.

# Apply Terraform.
${CLOUD_INFRASTRUCTURE_FOLDER}/Terraform/terraform_apply.sh {dev,test,prod}
```

Terraform is responsible for uploading the new JAR to S3 and update the Lambda function to reference the new code.
