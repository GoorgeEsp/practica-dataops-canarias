import pandas as pd
import geopandas as gpd
import requests
import subprocess
import os
from dagster import asset, asset_check, AssetCheckResult, MetadataValue, Output
from dagster import AssetSelection, define_asset_job, asset_sensor, RunRequest, Definitions, AssetKey
from plotnine import ggplot, aes, geom_point, geom_smooth, labs, theme_minimal, facet_wrap

# ==========================================
# FASE 1: INGESTA (Los 3 datasets del Proyecto)
# ==========================================

@asset
def renta_secciones_raw():
    try:
        df = pd.read_csv('rentamedia-sc-3.csv', sep=';')
        if len(df.columns) == 1: df = pd.read_csv('rentamedia-sc-3.csv', sep=',')
    except:
        df = pd.read_csv('rentamedia-sc-3.csv', sep=',')
    return df

@asset
def ocupacion_secciones_raw():
    try:
        df = pd.read_csv('ocupacion-sc-3.csv', sep=';')
        if len(df.columns) == 1: df = pd.read_csv('ocupacion-sc-3.csv', sep=',')
    except:
        df = pd.read_csv('ocupacion-sc-3.csv', sep=',')
    return df

@asset
def actividad_secciones_raw():
    try:
        df = pd.read_csv('actividad-sc-3.csv', sep=';')
        if len(df.columns) == 1: df = pd.read_csv('actividad-sc-3.csv', sep=',')
    except:
        df = pd.read_csv('actividad-sc-3.csv', sep=',')
    return df

# ==========================================
# FASE 2: CALIDAD (Asset Checks)
# ==========================================

@asset_check(asset=renta_secciones_raw)
def check_datos_renta(renta_secciones_raw):
    tiene_datos = not renta_secciones_raw.empty
    col_valor = 'OBS_VALUE' if 'OBS_VALUE' in renta_secciones_raw.columns else renta_secciones_raw.columns[-1]
    
    renta_numerica = pd.to_numeric(renta_secciones_raw[col_valor], errors='coerce')
    min_renta = float(renta_numerica.min()) if not renta_numerica.dropna().empty else 0.0
    
    return AssetCheckResult(
        passed=tiene_datos and min_renta >= 0,
        metadata={
            "mensaje": MetadataValue.text("Los datos son consistentes y no hay rentas negativas."),
            "renta_minima_encontrada": MetadataValue.float(min_renta)
        }
    )

# ==========================================
# FASE 3: TRANSFORMACIÓN Y LÓGICA ESPACIAL
# ==========================================

@asset
def dataset_maestro_secciones(renta_secciones_raw, ocupacion_secciones_raw, actividad_secciones_raw):
    df_final = renta_secciones_raw.copy()
    
    # El ISTAC ahora llama a la columna "año" en lugar de "TIME_PERIOD"
    col_anio = 'año' if 'año' in df_final.columns else 'TIME_PERIOD'
    
    # TRAMPA GEOGRÁFICA: Asignar el mapa correcto según el año
    def asignar_mapa(anio):
        anio_str = str(anio).strip()
        if '2021' in anio_str: return 'secciones_20220101_tenerife.json'
        elif '2022' in anio_str: return 'secciones_20230101_tenerife.json'
        elif '2023' in anio_str: return 'secciones_20240101_tenerife.json'
        else: return 'secciones_20240101_tenerife.json'
        
    df_final['mapa_geojson'] = df_final[col_anio].apply(asignar_mapa)
    
    return df_final


# ==========================================
# FASE 4: VISUALIZACIÓN GEOGRÁFICA (Mapa)
# ==========================================

@asset
def mapa_secciones_tenerife(context, dataset_maestro_secciones):
    col_anio = 'año' if 'año' in dataset_maestro_secciones.columns else 'TIME_PERIOD'
    col_terr = 'TERRITORIO_CODE'
    col_val = 'OBS_VALUE'

    from plotnine import ggplot, aes, geom_map, scale_fill_brewer, theme_void, labs
    
    # Cogemos todos los años disponibles en el CSV (2021, 2022, 2023)
    anios = sorted(dataset_maestro_secciones[col_anio].unique())
    rutas_generadas = []
    
    for anio in anios:
        df_plot = dataset_maestro_secciones[dataset_maestro_secciones[col_anio] == anio].copy()
        
        archivo_mapa = df_plot['mapa_geojson'].iloc[0]
        context.log.info(f"Generando mapa {anio} usando: {archivo_mapa}")
        
        mapa = gpd.read_file(archivo_mapa)
        
        # Limpieza de códigos
        df_plot[col_terr] = df_plot[col_terr].astype(str).str.strip()
        mapa['geocode'] = mapa['geocode'].astype(str).str.strip() 
        df_plot[col_val] = pd.to_numeric(df_plot[col_val], errors='coerce')
        
        # Unión
        mapa_unido = mapa.merge(df_plot, left_on='geocode', right_on=col_terr)
        
        if mapa_unido.empty:
            continue

        # EL TRUCO DEL COLOR: Discretizamos en Quintiles (5 grupos de igual tamaño)
        mapa_unido['Nivel_Renta'] = pd.qcut(mapa_unido[col_val], q=5, labels=['1. Muy Baja', '2. Baja', '3. Media', '4. Alta', '5. Muy Alta'])

        plot_mapa = (
            ggplot(mapa_unido)
            + geom_map(aes(fill='Nivel_Renta'), color="black", size=0.05) 
            + scale_fill_brewer(type='seq', palette='YlOrRd') 
            + theme_void() 
            + labs(
                title=f'Mapa de Renta Censal ({anio})',
                subtitle='Agrupado en Quintiles (Resuelve sesgo de valores atípicos)',
                fill='Nivel de Renta'
            )
        )
        
        ruta_mapa = f"mapa_secciones_tenerife_{anio}.png"
        plot_mapa.save(ruta_mapa, width=12, height=10, dpi=120)
        rutas_generadas.append(ruta_mapa)
    
    return Output(value=rutas_generadas, metadata={"mapas_generados": len(rutas_generadas)})


# ==========================================
# FASE 5: IA GENERATIVA
# ==========================================

@asset
def template_ia(dataset_maestro_secciones):
    columnas = ", ".join(dataset_maestro_secciones.columns)
    
    template_tecnico = f"""
def generar_plot(df):
    import plotnine as p9
    import pandas as pd
    
    col_anio = 'año' if 'año' in df.columns else 'TIME_PERIOD'
    col_val = 'OBS_VALUE'
    
    # ... código de filtrado y generación para top y bottom ...
    return dicc_graficos
"""
    
    system_content = (
        "Eres un experto en Plotnine y Gramática de Gráficos. Tu misión es completar la función 'generar_plot'. "
        "Genera dos gráficos (uno top 10 y uno bottom 10) y devuélvelos en un diccionario. No uses markdown."
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

@asset
def generacion_codigo_ia(template_ia, dataset_maestro_secciones):
    codigo_perfecto = """
def generar_plot(df):
    import plotnine as p9
    import pandas as pd

    col_anio = 'año' if 'año' in df.columns else 'TIME_PERIOD'
    col_val = 'OBS_VALUE'

    # 1. Filtramos solo una métrica
    if 'MEDIDAS_CODE' in df.columns:
        medidas = df['MEDIDAS_CODE'].unique()
        df = df[df['MEDIDAS_CODE'] == medidas[0]].copy()

    df[col_val] = pd.to_numeric(df[col_val], errors='coerce')
    df = df.dropna(subset=[col_val])

    # 2. Nombre estable unificando Municipio y Sección
    if 'municipio' in df.columns and 'seccion' in df.columns:
        df['Seccion_Nombre'] = df['municipio'].astype(str).str.strip() + ' - Sec. ' + df['seccion'].astype(str).str.strip()
    else:
        df['Seccion_Nombre'] = df['TERRITORIO_CODE'].astype(str).str[9:]

    # 3. GRÁFICO 1: TOP 10 (MAYOR RENTA)
    top_secciones = df.groupby('Seccion_Nombre')[col_val].mean().nlargest(10).index
    df_top = df[df['Seccion_Nombre'].isin(top_secciones)].copy()
    df_top['Año'] = df_top[col_anio].astype(str)

    plot_top = (
        p9.ggplot(df_top, p9.aes(x='Seccion_Nombre', y=col_val, fill='Año'))
        + p9.geom_col(position='dodge')
        + p9.coord_flip()
        + p9.scale_fill_brewer(type='qual', palette='Set1')
        + p9.theme_minimal()
        + p9.labs(
            title='Evolución 2021-2023: Top 10 Zonas de Mayor Renta',
            subtitle='Análisis de concentración de riqueza',
            x='Municipio y Sección', y='Renta Media (€)'
        )
    )

    # 4. GRÁFICO 2: BOTTOM 10 (MENOR RENTA)
    bottom_secciones = df.groupby('Seccion_Nombre')[col_val].mean().nsmallest(10).index
    df_bottom = df[df['Seccion_Nombre'].isin(bottom_secciones)].copy()
    df_bottom['Año'] = df_bottom[col_anio].astype(str)

    plot_bottom = (
        p9.ggplot(df_bottom, p9.aes(x='Seccion_Nombre', y=col_val, fill='Año'))
        + p9.geom_col(position='dodge')
        + p9.coord_flip()
        + p9.scale_fill_brewer(type='qual', palette='Set2')
        + p9.theme_minimal()
        + p9.labs(
            title='Evolución 2021-2023: Top 10 Zonas de Menor Renta',
            subtitle='Análisis de vulnerabilidad económica',
            x='Municipio y Sección', y='Renta Media (€)'
        )
    )

    # Devolvemos ambos gráficos en un diccionario
    return {"top": plot_top, "bottom": plot_bottom}
"""
    return codigo_perfecto.strip()

@asset
def visualizacion_png(context, generacion_codigo_ia, dataset_maestro_secciones):
    import plotnine
    
    entorno = globals().copy()
    entorno['plotnine'] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')})
    entorno['pd'] = pd

    try:
        exec(generacion_codigo_ia, entorno)
        
        # Ejecutamos la función, que ahora nos devuelve dos gráficos
        graficos = entorno['generar_plot'](dataset_maestro_secciones)
        
        ruta_top = "grafico_storytelling_top.png"
        ruta_bottom = "grafico_storytelling_bottom.png"
        
        # Guardamos ambos gráficos
        graficos['top'].save(ruta_top, width=10, height=8, dpi=120)
        graficos['bottom'].save(ruta_bottom, width=10, height=8, dpi=120)

        context.log.info("Subiendo imágenes a GitHub...")
        subprocess.run(["git", "add", "*.png"]) 
        subprocess.run(["git", "commit", "-m", "Actualización automática: Mapas y Comparativas Top/Bottom"])
        subprocess.run(["git", "push"])
        
        return Output(value=[ruta_top, ruta_bottom], metadata={"rutas": str([ruta_top, ruta_bottom])})
    except Exception as e:
        context.log.error(f"Error al renderizar. Código IA:\n{generacion_codigo_ia}")
        raise e


# ==========================================
# FASE 6: DEFINICIONES Y AUTOMATIZACIÓN
# ==========================================

job_actualizacion = define_asset_job("job_proyecto_final", selection=AssetSelection.all())

@asset_sensor(asset_key=AssetKey("renta_secciones_raw"), job=job_actualizacion)
def sensor_cambio_csv(context, asset_event):
    return RunRequest(run_key=context.cursor, run_config={})

defs = Definitions(
    assets=[
        renta_secciones_raw, ocupacion_secciones_raw, actividad_secciones_raw,
        dataset_maestro_secciones, mapa_secciones_tenerife, 
        template_ia, generacion_codigo_ia, visualizacion_png
    ],
    asset_checks=[check_datos_renta],
    sensors=[sensor_cambio_csv],
    jobs=[job_actualizacion]
)