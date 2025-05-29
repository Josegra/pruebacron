import pandas as pd
import os
from unidecode import unidecode

# Función para renombrar columnas duplicadas
def rename_duplicates(columns, target_col):
    count = 1
    new_columns = []
    for col in columns:
        if col == target_col:
            new_columns.append(f"{target_col}_{count}")
            count += 1
        else:
            new_columns.append(col)
    return new_columns

# URLs de las ligas a scrapear
urls = [
    'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats',
    'https://fbref.com/en/comps/10/stats/Championship-Stats',
    'https://fbref.com/en/comps/22/stats/Major-League-Soccer-Stats',
    'https://fbref.com/en/comps/24/stats/Serie-A-Stats',
    'https://fbref.com/en/comps/31/stats/Liga-MX-Stats',
    'https://fbref.com/en/comps/23/stats/Eredivisie-Stats',
    'https://fbref.com/en/comps/32/Primeira-Liga-Stats',
    'https://fbref.com/en/comps/21/Liga-Profesional-Argentina-Stats',
    'https://fbref.com/en/comps/18/Serie-B-Stats'
]

dataframes = []

# Scraping y limpieza básica
for url in urls:
    print(f"Descargando datos de: {url}")
    dfs = pd.read_html(url)
    if not dfs:
        print(f"Error: No se encontraron tablas en la URL: {url}")
        continue
    df = dfs[0]
    df.columns = df.columns.droplevel(0)
    df = df[df['Player'] != 'Player'].copy()
    dataframes.append(df)

# Unir todos los DataFrames
df_all = pd.concat(dataframes, ignore_index=True)
print(f"Total de filas tras concatenar ligas: {len(df_all)}")

# Renombrar columnas duplicadas
columns_to_rename_duplicates = ['Gls', 'Ast', 'G-PK', 'PK', 'PKatt', 'Sh', 'SoT', 'xG', 'npxG', 'xA', 'npxG+xA', 'G+A', 'xAG', 'npxG+xAG']
for col in columns_to_rename_duplicates:
    if col in df_all.columns:
        df_all.columns = rename_duplicates(df_all.columns, col)
    else:
        print(f"Advertencia: Columna '{col}' no encontrada para renombrar duplicados.")

# Crear columna PlSqu
if 'Player' in df_all.columns and 'Squad' in df_all.columns:
    df_all['PlSqu'] = df_all['Player'].astype(str) + df_all['Squad'].astype(str)

# Limpiar acentos y caracteres especiales
if 'Player' in df_all.columns:
    df_all['Player'] = df_all['Player'].astype(str).apply(unidecode)
if 'Squad' in df_all.columns:
    df_all['Squad'] = df_all['Squad'].astype(str).apply(unidecode)

# Eliminar columna Matches si existe
if 'Matches' in df_all.columns:
    df_all.drop(columns='Matches', inplace=True)

# Renombrado final con sufijos _p90
new_column_names = []
columns_to_add_p90_suffix = ['G+A-PK', 'xG+xAG']
for col_name in df_all.columns:
    if isinstance(col_name, str):
        if '_1' in col_name:
            new_column_names.append(col_name.replace('_1', ''))
        elif '_2' in col_name:
            new_column_names.append(col_name.replace('_2', '_p90'))
        elif col_name in columns_to_add_p90_suffix:
            new_column_names.append(col_name + '_p90')
        else:
            new_column_names.append(col_name)
    else:
        new_column_names.append(col_name)
df_all.columns = new_column_names

# Crear carpeta de salida
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)

# Guardar CSV con el nombre original
output_path = os.path.join(output_dir, 'big5_player_stats_standard.csv')
df_all.to_csv(output_path, index=False)

print(f"Datos combinados procesados y exportados correctamente a {output_path}")
print(f"Columnas finales: {df_all.columns.tolist()}")
print(df_all.head())
