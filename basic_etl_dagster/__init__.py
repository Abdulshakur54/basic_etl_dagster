from dagster import Definitions, load_assets_from_modules, EnvVar
from .resources import MySQLResource, MongoDBResource
from .assets import assets
from .jobs import update_diamond_in_mongodb_job
from .schedules import minutely_diamond_update_schedule

all_assets = load_assets_from_modules([assets])
all_jobs = [update_diamond_in_mongodb_job]
all_schedules = [minutely_diamond_update_schedule]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        "mysql_resource": MySQLResource(host=EnvVar('mysql_host'), username=EnvVar('mysql_username'), password=EnvVar('mysql_password')),
        "mongodb_resource": MongoDBResource(host=EnvVar('mongo_host'), username=EnvVar('mongo_username'), password=EnvVar('mongo_password'))
    }
)
