from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import practica_assets

defs = Definitions(
    assets=load_assets_from_modules([practica_assets]),
    asset_checks=load_asset_checks_from_modules([practica_assets])
)