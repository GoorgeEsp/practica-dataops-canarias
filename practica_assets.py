import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue
from plotnine import ggplot, aes, geom_point, geom_smooth, labs, theme_minimal, facet_wrap
import requests
from dagster import Output
import geopandas as gpd
from dagster import AssetSelection, define_asset_job, asset_sensor, RunRequest, Definitions, AssetKey
# --- ASSETS ---

@asset
def dataset_renta_bruto():
    df = pd.read_csv('distribucion-renta-canarias.csv')
    # Forzamos el fallo del check de carga borrando una columna importante
    #df = df.drop(columns=['MEDIDAS#es']) 
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
    
    # Forzamos el fallo de transformación añadiendo un municipio con un 150% de universitarios
    #fila_imposible = pd.DataFrame({"TERRITORIO_CODE": ["99999"], "Total_Total": [100], "Total_Sup": [150], "Tasa_Universidad": [150.0]})
    #df_unido = pd.concat([df_unido, fila_imposible], ignore_index=True)
    
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
    # Forzamos que sea un float nativo de Python
    tasa_maxima = float(dataset_estudios['Tasa_Universidad'].max())
    # Forzamos que sea un bool nativo de Python
    passed = bool(tasa_maxima <= 100.0)
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "tasa_maxima_encontrada": MetadataValue.float(tasa_maxima),
            "mensaje": "Error matemático: Hay tasas de universitarios superiores al 100%."
        }
    )


import requests
from dagster import Output

# --- IA GENERATIVA ---

# 1. PLANTILLA: Le decimos a la IA qué queremos
@asset
def template_ia(dataset_renta_bruto):
    columnas = ", ".join(dataset_renta_bruto.columns)
    
    template_tecnico = f"""
def generar_plot(df):
    import plotnine as p9
    import pandas as pd
    
    # 1. FILTRADO INTELIGENTE (Para que no salga vacía)
    # Seleccionamos el año más reciente que haya en el dataset
    ultimo_año = df['TIME_PERIOD#es'].max()
    df_plot = df[df['TIME_PERIOD#es'] == ultimo_año].copy()
    
    # Si existe la columna de medidas, cogemos la primera disponible para no duplicar barras
    if 'MEDIDAS_CODE' in df_plot.columns:
        medida = df_plot['MEDIDAS_CODE'].unique()[0]
        df_plot = df_plot[df_plot['MEDIDAS_CODE'] == medida]

    # 2. CREACIÓN DEL GRÁFICO (Renta por Municipio)
    plot = (
        p9.ggplot(df_plot, p9.aes(x='TERRITORIO#es', y='OBS_VALUE', fill='TERRITORIO#es'))
        + p9.geom_col() # Gráfico de barras para comparar municipios
        + p9.scale_fill_manual(values=['#007bff' if 'Tenerife' in str(t) else '#D3D3D3' for t in df_plot['TERRITORIO#es'].unique()])
        + p9.theme_minimal()
        + p9.theme(
            axis_text_x=p9.element_text(rotation=90, hjust=1), # Rotamos nombres para que se lean
            legend_position='none'
        )
        + p9.labs(
            title=f'Renta por Municipio - Año {{ultimo_año}}',
            x='Municipio',
            y='Renta (€)'
        )
    )
    return plot
"""
    
    system_content = (
        "Eres un experto en Plotnine. Tu única misión es completar la función 'generar_plot'. "
        "Usa el Municipio (TERRITORIO#es) en el eje X y el Valor (OBS_VALUE) en el eje Y. "
        "No uses markdown ni texto extra."
    )
    
    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Columnas: {columnas}. Completa este código:\n{template_tecnico}"}
        ],
        "temperature": 0.0, 
        "max_tokens": 1000
    }


# 2. GENERACIÓN: Conectamos con la API de la Universidad
@asset
def generacion_codigo_ia(template_ia, dataset_renta_bruto):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-1234"
    }
    
    response = requests.post(url, headers=headers, json=template_ia)
    
    if response.status_code != 200:
        raise Exception(f"El servidor IA ha rechazado la petición: {response.text}")
        
    respuesta = response.json()
    codigo_bruto = respuesta["choices"][0]["message"]["content"]
    codigo_limpio = codigo_bruto.replace("```python", "").replace("```", "").strip()
    
    columnas_reales = dataset_renta_bruto.columns.tolist()
    columnas_en_codigo = [col for col in columnas_reales if col in codigo_limpio]
    
    if not columnas_en_codigo:
        raise Exception(
            f"La IA generó código con columnas inventadas.\n"
            f"Columnas reales: {columnas_reales}\n"
            f"Código generado:\n{codigo_limpio}"
        )
    
    return codigo_limpio

# 3. EJECUCIÓN: Dagster ejecuta el código generado por la IA y lo sube a GitHub
@asset
def visualizacion_png(context, generacion_codigo_ia, dataset_renta_bruto):
    import plotnine
    import subprocess
    
    entorno_ejecucion = globals().copy()
    entorno_ejecucion['plotnine'] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')
    })
    entorno_ejecucion['pd'] = pd

    try:
        exec(generacion_codigo_ia, entorno_ejecucion)
        grafico = entorno_ejecucion['generar_plot'](dataset_renta_bruto)
        
        ruta_archivo = "visualizacion_ia.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)

        context.log.info("Subiendo imagen a GitHub...")
        subprocess.run(["git", "add", ruta_archivo])
        subprocess.run(["git", "commit", "-m", "Actualización automática del gráfico IA"])
        subprocess.run(["git", "push"])
        
        return Output(
            value=ruta_archivo,
            metadata={"ruta": ruta_archivo, "mensaje": "Gráfico generado y subido a GitHub"}
        )
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}\n\nCódigo de la IA:\n{generacion_codigo_ia}")
        raise e


import geopandas as gpd

@asset
def mapa_rentas_canarias(context, dataset_renta_bruto):
    # 1. Cargamos el mapa
    mapa = gpd.read_file('Municipios-2024.json')
    
    # 2. Preparamos los datos del CSV
    df_renta = dataset_renta_bruto.copy()
    
    # Cogemos el último año disponible
    ultimo_año = str(df_renta['TIME_PERIOD#es'].max())
    df_renta = df_renta[df_renta['TIME_PERIOD#es'].astype(str) == ultimo_año]
    
    # --- ¡IMPORTANTE!: Filtramos para que solo haya UNA fila por municipio ---
    # Cogemos solo una medida (por ejemplo, 'BRUTA') para evitar que el mapa sea gris
    if 'MEDIDAS_CODE' in df_renta.columns:
        medida_principal = df_renta['MEDIDAS_CODE'].unique()[0]
        df_renta = df_renta[df_renta['MEDIDAS_CODE'] == medida_principal]
    
    # 3. Forzamos que la Renta sea un NÚMERO (si es texto, sale gris)
    df_renta['OBS_VALUE'] = pd.to_numeric(df_renta['OBS_VALUE'], errors='coerce')
    # Quitamos filas sin datos
    df_renta = df_renta.dropna(subset=['OBS_VALUE'])
    
    # 4. Limpieza de códigos para la unión
    mapa['geocode'] = mapa['geocode'].astype(str).str.strip().str.zfill(5)
    df_renta['TERRITORIO_CODE'] = df_renta['TERRITORIO_CODE'].astype(str).str.strip().str.zfill(5)
    
    # 5. Unión (Merge)
    mapa_unido = mapa.merge(df_renta, left_on='geocode', right_on='TERRITORIO_CODE')
    
    if mapa_unido.empty:
        raise Exception("No se han podido unir los datos. Revisa los códigos de municipio.")

    # 6. Dibujo del mapa coloreado
    from plotnine import ggplot, aes, geom_map, scale_fill_distiller, theme_void, labs
    
    plot_mapa = (
        ggplot(mapa_unido)
        + geom_map(aes(fill='OBS_VALUE')) 
        # Cambiamos type='div' por type='seq' porque YlOrRd es una secuencia
        + scale_fill_distiller(type='seq', palette='YlOrRd', direction=1) 
        + theme_void() 
        + labs(
            title=f'Distribución de Renta Bruta ({ultimo_año})',
            subtitle='Mapa municipal de las Islas Canarias',
            fill='Euros'
        )
    )
    
    # 7. Guardamos
    ruta_mapa = "mapa_canarias.png"
    plot_mapa.save(ruta_mapa, width=12, height=8, dpi=100)
    
    context.log.info(f"¡Mapa coloreado con éxito! Se han pintado {len(mapa_unido)} municipios.")
    return Output(value=ruta_mapa, metadata={"municipios_coloreados": len(mapa_unido)})







# 1. DEFINIMOS EL JOB (Tiene que ir PRIMERO)
job_actualizacion = define_asset_job(
    "job_actualizacion_completa", 
    selection=AssetSelection.all()
)

# 2. DEFINIMOS EL SENSOR (Usa el job de arriba)
@asset_sensor(asset_key=AssetKey("dataset_renta_bruto"), job=job_actualizacion)
def sensor_cambio_csv(context, asset_event):
    return RunRequest(
        run_key=context.cursor,
        run_config={},
    )

# 3. DEFINIMOS EL OBJETO DE DEFINICIONES (Para que Dagster vea todo)
defs = Definitions(
    assets=[
        dataset_renta_bruto, 
        dataset_islas, 
        dataset_estudios, 
        template_ia, 
        generacion_codigo_ia, 
        visualizacion_png, 
        mapa_rentas_canarias
    ],
    asset_checks=[check_columnas_renta, check_tasa_logica],
    sensors=[sensor_cambio_csv],
    jobs=[job_actualizacion]
)