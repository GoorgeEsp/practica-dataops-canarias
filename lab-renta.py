import pandas as pd
from plotnine import ggplot, aes, geom_histogram, labs, theme_minimal

# 1. Cargar los datos
df = pd.read_csv('distribucion-renta-canarias.csv')

# 2. Filtrar los datos para hacer el gráfico:
# Vamos a coger solo el año 2019 y el concepto de "Sueldos y salarios"
df_filtrado = df[(df['MEDIDAS#es'] == 'Sueldos y salarios') & (df['TIME_PERIOD#es'] == 2019)]

# 3. Construir la visualización (Gramática de Gráficos)
grafico = (
    ggplot(df_filtrado, aes(x='OBS_VALUE')) +
    geom_histogram(fill='steelblue', color='black', bins=15) +
    labs(
        title='Distribución del porcentaje de ingresos por Sueldos en Canarias (2019)',
        x='Porcentaje de ingresos (%)',
        y='Frecuencia (Territorios)'
    ) +
    theme_minimal()
)

# 4. Guardar el gráfico en la carpeta
grafico.save("histograma_rentas.png", width=8, height=6, dpi=300)

print("El gráfico se ha generado y guardado como 'histograma_rentas.png'")