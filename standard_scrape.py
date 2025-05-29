import pandas as pd
import os
from unidecode import unidecode # <--- AÑADIDO: Importar unidecode

# Función para renombrar columnas duplicadas (tal como la proporcionaste)
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

# --- COMIENZO DEL SCRIPT ORIGINAL ---
# Leer la tabla de estadísticas desde FBref
url = 'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats'
print(f"Descargando datos de: {url}")
dfs = pd.read_html(url)
if not dfs:
    print("Error: No se encontraron tablas en la URL.")
    exit()
df = dfs[0]

# Eliminar el encabezado duplicado
df.columns = df.columns.droplevel(0)

# Limpiar filas que contienen el encabezado repetido
df_cleaned = df[df['Player'] != 'Player'].copy() # Usar .copy() para evitar SettingWithCopyWarning
print(f"Filas iniciales después de la limpieza básica: {len(df_cleaned)}")

# --- NUEVAS OPERACIONES DE LIMPIEZA ---

# 1. Renombrar columnas duplicadas especificadas
# Asumimos que las operaciones de limpieza se aplican a df_cleaned
print("Renombrando columnas duplicadas (Gls, Ast, etc.)...")
columns_to_rename_duplicates = ['Gls', 'Ast', 'G-PK', 'PK', 'PKatt', 'Sh', 'SoT', 'xG', 'npxG', 'xA', 'npxG+xA', 'G+A', 'xAG', 'npxG+xAG'] # Añadí PK, PKatt, Sh, SoT por si también tienen duplicados comunes
for col in columns_to_rename_duplicates:
    if col in df_cleaned.columns: # Verificar si la columna existe antes de intentar renombrar
        df_cleaned.columns = rename_duplicates(df_cleaned.columns, col)
    else:
        print(f"Advertencia: La columna '{col}' no se encontró para renombrar duplicados.")

# 2. Limpieza adicional de columnas
print("Realizando limpieza adicional de columnas...")
if 'Player' in df_cleaned.columns and 'Squad' in df_cleaned.columns:
    df_cleaned.loc[:, 'PlSqu'] = df_cleaned['Player'] + df_cleaned['Squad']
else:
    print("Advertencia: Columnas 'Player' o 'Squad' no encontradas para crear 'PlSqu'.")

if 'Player' in df_cleaned.columns:
    df_cleaned.loc[:, 'Player'] = df_cleaned['Player'].astype(str).apply(unidecode) # Convertir a str por si hay no-strings
else:
    print("Advertencia: Columna 'Player' no encontrada para aplicar unidecode.")

if 'Squad' in df_cleaned.columns:
    df_cleaned.loc[:, 'Squad'] = df_cleaned['Squad'].astype(str).apply(unidecode) # Convertir a str por si hay no-strings
else:
    print("Advertencia: Columna 'Squad' no encontrada para aplicar unidecode.")

if 'Matches' in df_cleaned.columns:
    print("Eliminando columna 'Matches'.")
    df_cleaned.drop(columns='Matches', inplace=True)

# 3. Renombrar columnas finales (ej. para sufijos _p90)
print("Aplicando renombrado final de columnas (sufijos _p90)...")
new_column_names = []
columns_to_add_p90_suffix = ['G+A-PK', 'xG+xAG'] # Columnas que específicamente deben tener _p90 si no son _1 o _2

for col_name in df_cleaned.columns:
    if isinstance(col_name, str): # Aplicar solo si el nombre de la columna es string
        if '_1' in col_name:
            new_column_names.append(col_name.replace('_1', ''))
        elif '_2' in col_name:
            new_column_names.append(col_name.replace('_2', '_p90'))
        elif col_name in columns_to_add_p90_suffix:
            new_column_names.append(col_name + '_p90')
        else:
            new_column_names.append(col_name)
    else:
        new_column_names.append(col_name) # Mantener nombres de columna no string tal cual

df_cleaned.columns = new_column_names

# --- FIN DE NUEVAS OPERACIONES ---

# Crear carpeta de salida si no existe
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)

# Exportar a CSV
output_path = os.path.join(output_dir, 'big5_player_stats_standard.csv')
df_cleaned.to_csv(output_path, index=False)

print(f"Datos procesados y exportados correctamente a {output_path}")
print(f"Columnas finales: {df_cleaned.columns.tolist()}")
print(f"Primeras filas del DataFrame final:\n{df_cleaned.head()}")
