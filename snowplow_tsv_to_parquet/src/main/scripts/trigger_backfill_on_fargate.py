import os
from datetime import datetime, timedelta

import time
import boto3

# Run this script from shell to trigger snow-plow-tsv-to-parquet tasks on fargate.

# Beware to set environment env_name to 'dev' or 'prod'. To backfill production set environment env_name to 'prod'
# and assume prod permissions by ~/assume_aws_prod

# In shell for prod  :
#           ~/assume_aws_prod python trigger_backfill_on_fargate.py 'yyyy-mm-dd' 'yyyy-mm-dd' &>> prod/LOGFILE.txt
# or for dev:
#           python trigger_backfill_on_fargate.py 'yyyy-mm-dd' 'yyyy-mm-dd' &>> dev/LOGFILE.txt

# Use &>> to append the cumulative redirection of stdout and stderr to the file

# Let the period be of ~7 days. Longer periods may overflow the limits for starting tasks per unit of time on FARGATE.
# Rerun failed tasks: Check for failures written to logfile. Occasionally, failures do not get logged. Check Cloudwatch
# Log Group /aws/lambda/<environment>-failed-deployments-lambda --> Search log group "FAILED" and look for
# snowplow-tsv-to-parquet. <environment> may be Dev or Prod. Finding the related "Received ECS task (...)" you can
# check the event details json to contain "'exitCode': 1" confirming it is the failed task and
# "'command': ['2018', '4', '17', '0']" telling which date and hour has failed.

# Failures also appear on Grafana, though it is not possible to check which command (date, hour) has failed:
# http://grafana.dev.aws.jyllands-posten.dk/d/KF-C4prmz/container-failures?orgId=1

# Public subnets for each cluster's VPC.
env_to_subnets = {
    'dev': ['subnet-c86354ad', 'subnet-c30e51b4', 'subnet-c59fe89c'],
    'test': ['subnet-75e30611', 'subnet-26aaa851', 'subnet-fd96cca4'],
    'prod': ['subnet-3862555d', 'subnet-080d527f', 'subnet-089ee951']
}

# The ECS cluster's EC2 instances' security group id.
env_to_security_group = {
    'dev': 'sg-13fa6974',
    'test': 'sg-a73c59c0',
    'prod': 'sg-f46f3593'
}

env_name = 'dev' #os.environ['ENVIRONMENT_NAME'].lower()
subnets = env_to_subnets[env_name]
security_group = env_to_security_group[env_name]

ecs_cluster = env_name.capitalize()

def run_ecs_and_wait(**kwargs):
    ecs = boto3.client('ecs', region_name='eu-west-1')

    response = ecs.run_task(**kwargs)

    if 'tasks' not in response or len(response['tasks']) == 0:
        raise Exception('No task returned by AWS ECS. Failure reason: {}'.format(response['failures'][0]['reason']))

    task_arn = response['tasks'][0]['taskArn']

    if response['tasks'][0]['desiredStatus'] != 'RUNNING':
        raise Exception('Could not start ecs task: {}, status = {}'
                        .format(task_arn, response['tasks'][0]['desiredStatus']))

    while response['tasks'][0]['desiredStatus'] == 'RUNNING':
        time.sleep(10)

        response = ecs.describe_tasks(
            cluster=kwargs.get('cluster'),
            tasks=[task_arn])

    try:
        if response['tasks'][0]['containers'][0]['exitCode'] != 0:
            print('ERROR: exitcode: = {}; task-arn: {}.  kwargs[overrides]: {}at: {}\n'
                  .format(response['tasks'][0]['containers'][0]['exitCode'], task_arn, kwargs.get('overrides'), datetime.now()))
        else:
            print('SUCCES: task arn {}.  kwargs[overrides]: {}at: {}\n'.format(task_arn, kwargs.get('overrides'), datetime.now()))
    except Exception as ex:
        print('ERROR: EXCEPTION: No exitcode. taskARN: {}Exception: {} kwargs[overrides]: {}at: {}\n'.format(task_arn, ex, kwargs.get('overrides'), datetime.now()))


def run_on_fargate(task_definition, command):

    run_ecs_and_wait(
        cluster=ecs_cluster,
        taskDefinition=task_definition,
        startedBy='Airflow',
        overrides={
            'containerOverrides': [
                {
                    'name': task_definition,
                    'command': command,
                }
            ]
        },
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': subnets,
                'securityGroups': [security_group],
                'assignPublicIp': 'ENABLED',
            }
        }
    )


def tsv_to_parquet(execution_date, **kwargs):
    ecs_task_definition = '{}-snowplow-tsv-to-parquet'.format(ecs_cluster)

    # The DAG is triggered a few minutes after every hour but the execution date is for the previous hour (that's how
    # Airflow's scheduler operates). Therefore, we can simply send the execution_date date the to ECS to get the
    # correct hour processed. For example, the run time (datetime.now()) is 20180705 08:05 but execution_date will be
    # 20180705 07:05.

    command = [str(execution_date.year), str(execution_date.month), str(execution_date.day), str(execution_date.hour)]
    run_on_fargate(ecs_task_definition, command)


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


import sys
from multiprocessing import Pool

if __name__ == '__main__':

    start_date_str = sys.argv[1]
    end_date_str = sys.argv[2]

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')  # start date
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')  # end date

    print("BACKFILLING FOR PERIOD {} to {}".format(start_date_str, end_date_str))
    # pool is 40 to avoid overflow of aws ecs service limits
    # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service_limits.html
    with Pool(40) as p:
        p.map(tsv_to_parquet, get_list_of_dates_and_hours_between(start_date, end_date))

