import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue
from plotnine import ggplot, aes, geom_point, geom_smooth, labs, theme_minimal, facet_wrap

# --- ASSETS ---

@asset
def dataset_renta_bruto():
    df = pd.read_csv('distribucion-renta-canarias.csv')
    # PASO 8: Forzamos el fallo del check de carga borrando una columna importante
    df = df.drop(columns=['MEDIDAS#es']) 
    return df

@asset
def dataset_islas():
    df = pd.read_csv('codislas.csv', sep=';', encoding='latin1')
    df['TERRITORIO_CODE'] = df['CPRO'].astype(str) + df['CMUN'].astype(str).str.zfill(3)
    return df

@asset
def dataset_estudios():
    df = pd.read_excel('nivelestudios.xlsx')
    df['TERRITORIO_CODE'] = df['Municipios de 500 habitantes o más'].str.split(' ').str[0]
    
    df_total = df[df['Nivel de estudios en curso'] == 'Total'].groupby('TERRITORIO_CODE')['Total'].sum().reset_index()
    df_sup = df[df['Nivel de estudios en curso'] == 'Educación superior'].groupby('TERRITORIO_CODE')['Total'].sum().reset_index()
    
    df_unido = pd.merge(df_total, df_sup, on='TERRITORIO_CODE', suffixes=('_Total', '_Sup'))
    df_unido['Tasa_Universidad'] = (df_unido['Total_Sup'] / df_unido['Total_Total']) * 100
    
    # PASO 8: Forzamos el fallo de transformación añadiendo un municipio con un 150% de universitarios
    fila_imposible = pd.DataFrame({"TERRITORIO_CODE": ["99999"], "Total_Total": [100], "Total_Sup": [150], "Tasa_Universidad": [150.0]})
    df_unido = pd.concat([df_unido, fila_imposible], ignore_index=True)
    
    return df_unido


# --- CHECKS ---

# 1. Check de Carga
@asset_check(asset=dataset_renta_bruto)
def check_columnas_renta(dataset_renta_bruto):
    columnas_esperadas = ['OBS_VALUE', 'MEDIDAS#es']
    columnas_presentes = dataset_renta_bruto.columns.tolist()
    
    # Comprobamos si todas las esperadas están en el dataframe
    todas_presentes = all(col in columnas_presentes for col in columnas_esperadas)
    
    return AssetCheckResult(
        passed=todas_presentes,
        metadata={
            "mensaje": "Faltan columnas vitales para el filtrado.",
            "columnas_actuales": MetadataValue.text(str(columnas_presentes))
        }
    )

# 2. Check de Transformación
@asset_check(asset=dataset_estudios)
def check_tasa_logica(dataset_estudios):
    # La tasa máxima no debe ser mayor a 100
    tasa_maxima = dataset_estudios['Tasa_Universidad'].max()
    passed = tasa_maxima <= 100.0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "tasa_maxima_encontrada": MetadataValue.float(tasa_maxima),
            "mensaje": "Error matemático: Hay tasas de universitarios superiores al 100%."
        }
    )