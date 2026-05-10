
from dagster import Definitions
import practica_assets

# Esto le dice a Dagster: "Coge todo lo que hemos definido en el otro archivo"
defs = Definitions(
    assets=[
        practica_assets.dataset_renta_bruto,
        practica_assets.dataset_islas,
        practica_assets.dataset_estudios,
        practica_assets.template_ia,
        practica_assets.generacion_codigo_ia,
        practica_assets.visualizacion_png,
        practica_assets.mapa_rentas_canarias,
    ],
    asset_checks=[
        practica_assets.check_columnas_renta,
        practica_assets.check_tasa_logica
    ],
    jobs=[practica_assets.job_actualizacion],
    sensors=[practica_assets.sensor_cambio_csv],
)