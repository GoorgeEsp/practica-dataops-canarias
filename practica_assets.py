import pandas as pd
from dagster import asset, MaterializeResult
from plotnine import ggplot, aes, geom_point, geom_smooth, labs, theme_minimal, facet_wrap

# ASSET 1: Rentas
@asset
def dataset_renta_bruto():
    return pd.read_csv('distribucion-renta-canarias.csv')

# ASSET 2: Islas
@asset
def dataset_islas():
    df = pd.read_csv('codislas.csv', sep=';', encoding='latin1')
    df['TERRITORIO_CODE'] = df['CPRO'].astype(str) + df['CMUN'].astype(str).str.zfill(3)
    return df

# ASSET 3: Estudios (NUEVO)
@asset
def dataset_estudios():
    """Carga el excel de estudios y calcula la tasa de educación superior por municipio."""
    # Leemos el Excel
    df = pd.read_excel('nivelestudios.xlsx')
    
    # Extraemos el código de municipio (separa "35001 Agaete" y se queda con "35001")
    df['TERRITORIO_CODE'] = df['Municipios de 500 habitantes o más'].str.split(' ').str[0]
    
    # Calculamos el total de población y el total de universitarios
    df_total = df[df['Nivel de estudios en curso'] == 'Total'].groupby('TERRITORIO_CODE')['Total'].sum().reset_index()
    df_sup = df[df['Nivel de estudios en curso'] == 'Educación superior'].groupby('TERRITORIO_CODE')['Total'].sum().reset_index()
    
    # Unimos y calculamos porcentaje
    df_unido = pd.merge(df_total, df_sup, on='TERRITORIO_CODE', suffixes=('_Total', '_Sup'))
    df_unido['Tasa_Universidad'] = (df_unido['Total_Sup'] / df_unido['Total_Total']) * 100
    
    return df_unido

# ASSET 4: Gran Unión Final (Rentas + Islas + Estudios)
@asset
def dataset_final(dataset_renta_bruto: pd.DataFrame, dataset_islas: pd.DataFrame, dataset_estudios: pd.DataFrame):
    """Filtra y cruza absolutamente todos los datos."""
    df_filtrado = dataset_renta_bruto[
        (dataset_renta_bruto['MEDIDAS#es'] == 'Sueldos y salarios') & 
        (dataset_renta_bruto['TIME_PERIOD#es'] == 2019)
    ]
    
    # Cruzamos Rentas con Islas
    paso1 = pd.merge(df_filtrado, dataset_islas, on='TERRITORIO_CODE', how='inner')
    
    # Cruzamos el resultado con los Estudios
    paso2 = pd.merge(paso1, dataset_estudios, on='TERRITORIO_CODE', how='inner')
    
    return paso2

# ASSET 5: Visualización Definitiva
@asset
def grafico_renta_vs_estudios(dataset_final: pd.DataFrame):
    """Genera un gráfico de dispersión cruzando renta de trabajo y estudios."""
    grafico = (
        ggplot(dataset_final, aes(x='Tasa_Universidad', y='OBS_VALUE', color='ISLA')) +
        geom_point(size=3, alpha=0.7) +
        geom_smooth(method='lm', color='black', linetype='dashed', se=False) + # Línea de tendencia
        facet_wrap('~ISLA') +
        labs(
            title='Correlación: Sueldos vs Educación Superior por Municipio (2019)',
            x='Tasa de Población con Educación Superior (%)',
            y='Ingresos por Sueldos (%)',
            color='Isla'
        ) +
        theme_minimal()
    )
    
    grafico.save("grafico_final_completo.png", width=12, height=8, dpi=300)
    
    return MaterializeResult(
        metadata={
            "filas_finales": len(dataset_final),
            "ruta_imagen": "grafico_final_completo.png"
        }
    )