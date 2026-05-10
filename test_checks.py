import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue

@asset
def islas_raw():
# Vamos a leer un archivo que sí tenemos
    df = pd.read_csv("distribucion-renta-canarias.csv")
    # Renombramos la columna para engañar al check
    df.rename(columns={"TERRITORIO#es": "isla"}, inplace=True)

    # EJERCICIO 6: Las filas comentadas las dejaremos para luego
    fila_sucia = pd.DataFrame({"año": [2022], "isla": ["tenerife"], "medida": ["gasto"], "valor": [1840000]})
    return pd.concat([df, fila_sucia], ignore_index=True)
    return df

@asset_check(asset=islas_raw)
def check_estandarizacion_islas(islas_raw):
    # Contamos categorías únicas originales vs normalizadas
    originales = islas_raw['isla'].nunique()
    normalizadas = islas_raw['isla'].str.capitalize().nunique()
    
    passed = originales == normalizadas
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "mensaje": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )