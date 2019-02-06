# Snowplow TSV to Parquet
Convert Snowplow TSV files to Parquet format while also extracting some information from the custom contexts to their own columns for faster lookup.

# Environment variables
```bash
IN_BUCKET=jpmedier-datalake-dev
OUT_BUCKET=behavior-datalake-dev
PARTITION_DATABASE=snowplow-dev
ATHENA_OUTPUT_LOCATION=aws-athena-query-results-092102721606-eu-west-1/snowplow-partitions
```

# Compiling in IntelliJ IDEA
The default settings for the Scala Compile Server in IntelliJ IDEA will result in compilation errors due to the compiler crashing when type checking the code. To avoid this, change the `Xss` JVM option for the Scala Compile Server to `2m`. Go to `Settings` (`CTRL+ALT+S`) and change the `JVM options` setting for the Scala Compile Server:

![Scala Compile Server Settings](readme_intellij_scala_compile_server_settings.png "Scala Compile Server Settings")

# Adding new columns to Athena/Glue
After adding a new column to the ContextExploder and Schemas, go to AWS Glue, select your database and table, and choose Edit Schema.
As we're using parquet files (read by name, default), you can freely add columns anywhere in the table. The new column will push the old ones, so what used to be 124 will now be 125 in the example below.

The new column will be queryable if the data exists, otherwise it won't return anything. The rest of the table will work as expected either way.

You can choose to disable the tsv-to-parquet task on Airflow while updating the schema, to ensure no tasks are running while the change is going through.
![AWS Glue Adding Columns](readme_aws_glue_add_column.png "AWS Glue Adding Columns")

# Backfilling using IntelliJ IDEA
If backfilling of snowplow-tsv-to-parquet is needed, following should be done:

1. Set environment variables
```bash
AWS_ASSUME_ROLE_ARN=arnForProdDeveloperRole
AWS_ROLE_SESSION_NAME=yourOwnSessionName
```
2. Setup LocalMain with the needed dates to process/backfill. 
3. Run LocalMain (with required environment variables from the top.)

# Backfilling using Airflow
SSH into the ECS/EC2 machine running the Airflow Docker container and execute an Airflow CLI command inside the Docker container.

```bash
# SSH into the ECS machine.
ssh -i "XXX.pem" ec2-user@YYY
# Find the name or the container id of the Airflow container.
docker ps
# Start a bash shell inside the container.
docker exec -it ecs-Test-airflow-ZZZ /bin/bash
# Execute Airflow CLI clear command.
airflow clear ...
```

For example, the following command will clear the `snowplow_tsv_to_parquet` task (and not its upstream or downstream tasks) of the `snowplow_tsv_to_parquet` DAG for a given time range. The CLI will ask for confirmation but you can skip that by adding `--no_confirm`.
```bash
airflow clear -t snowplow_tsv_to_parquet -s 2019-02-05T00:00:00 -e 2019-02-06T00:00:00 snowplow_tsv_to_parquet
```

Note that Airflow uses an obscene amount of memory to keep track of scheduled and running tasks, so it will probably crash if you clear, for example, a day's worth of tasks. You can partly get around this by increasing Airflow's memory reservation in ECS.
