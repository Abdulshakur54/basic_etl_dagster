from dagster import ScheduleDefinition
from ..jobs import update_diamond_in_mongodb_job
minutely_diamond_update_schedule = ScheduleDefinition(
    job=update_diamond_in_mongodb_job,
    cron_schedule="* * * * *" #Every minute
)