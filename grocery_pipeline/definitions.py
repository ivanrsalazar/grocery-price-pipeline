from dagster import Definitions
from grocery_pipeline.assets.bronze.hello_world import hello_world

defs = Definitions(
    assets=[hello_world],
)