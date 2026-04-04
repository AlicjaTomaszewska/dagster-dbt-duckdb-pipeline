"""Dagster entrypoint: load asset definitions and shared resources."""

from dagster import Definitions, load_assets_from_modules

import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": assets.dbt_resource,
    },
)
