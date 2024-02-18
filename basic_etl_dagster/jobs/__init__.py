from dagster import AssetSelection, define_asset_job

update_diamond_in_mongodb_job = define_asset_job(
    name="update_diamond_in_mongodb_job",
    selection=AssetSelection.keys("coll_diamonds", "tbl_diamonds")
)
