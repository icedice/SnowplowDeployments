import sys
import traceback
from datetime import datetime, timedelta
from multiprocessing import Pool

import boto3


# Method for retrieving list with every hour for every day for a period of time.
# period_start_date is inclusive
# period_end_date is exclusive
def get_list_of_dates_and_hours_between(period_start_date, period_end_date):
    delta = period_end_date - period_start_date
    dates_and_hours_list = []

    for days in range(delta.days):
        date = period_start_date + timedelta(days=days)
        for hours in range(24):
            date_and_hour = date + timedelta(hours=hours)
            dates_and_hours_list.append(date_and_hour)

    return dates_and_hours_list


def tsv_to_parquet(execution_date):
    add_batch_job(str(execution_date.year),
                  str(execution_date.month),
                  str(execution_date.day),
                  str(execution_date.hour))


# noinspection PyBroadException
def add_batch_job(year, month, day, hour):
    try:
        batch = boto3.client('batch', region_name='eu-west-1')

        command = f'{year} {int(month):02d} {int(day):02d} {int(hour):02d}'
        job_name = f'snowplow-tsv-to-parquet-{command.replace(" ","-")}'
        job_queue = f'{env_name}-snowplow-tsv-to-parquet-queue'
        job_definition = f'{env_name}-snowplow-tsv-to-parquet-definition'
        command = command.split()

        submit_job_response = batch.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            containerOverrides={'command': command}
        )

        job_id = submit_job_response['jobId']
        print(f'Submitted job {job_name} {job_id} to the job queue {job_queue}')

    except Exception:
        print(f"failed to add job {job_name} {job_id} to the job queue {job_queue}'")
        traceback.print_exc()


# Method for retrieving list with every hour for every day for a period of time.
# period_start_date is inclusive
# period_end_date is exclusive
if __name__ == '__main__':
    env_name = sys.argv[1]
    start_date_str = sys.argv[2]
    end_date_str = sys.argv[3]

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')  # start date
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')  # end date

    print("BACKFILLING FOR PERIOD {} to {}".format(start_date_str, end_date_str))
    # Pool is 5 to avoid overflow of aws api request limits. Usually AWS Api request
    # limits are maximum 5 request pr sec. This might be able to run at a higher pool if needed.
    with Pool(5) as p:
        p.map(tsv_to_parquet, get_list_of_dates_and_hours_between(start_date, end_date))
