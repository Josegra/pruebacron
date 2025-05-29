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

# --- CONFIGURACIÓN DE LIGAS A SCRAPEAR ---
league_data = [
    {
        "name": "Big 5 European Leagues",
        "url": 'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats',
        "add_comp_col": False, # Esta URL ya tiene la columna 'Comp'
        "comp_name": "" 
    },
    {
        "name": "Championship Inglaterra",
        "url": 'https://fbref.com/en/comps/10/stats/Championship-Stats',
        "add_comp_col": True,
        "comp_name": "ENG-Championship"
    },
    {
        "name": "MLS",
        "url": 'https://fbref.com/en/comps/22/stats/Major-League-Soccer-Stats',
        "add_comp_col": True,
        "comp_name": "USA-Major League Soccer"
    },
    {
        "name": "Brazil Serie A",
        "url": 'https://fbref.com/en/comps/24/stats/Serie-A-Stats', # Asume Serie A de Brasil
        "add_comp_col": True,
        "comp_name": "BRA-Serie A"
    },
    {
        "name": "Liga MX Mexico",
        "url": 'https://fbref.com/en/comps/31/stats/Liga-MX-Stats',
        "add_comp_col": True,
        "comp_name": "MEX-Liga MX"
    },
    {
        "name": "Eredivisie Holanda",
        "url": 'https://fbref.com/en/comps/23/stats/Eredivisie-Stats',
        "add_comp_col": True,
        "comp_name": "NED-Eredivisie"
    },
    {
        "name": "Primeira Liga Portugal",
        "url": 'https://fbref.com/en/comps/32/stats/Primeira-Liga-Stats', # Corregido para incluir /stats/
        "add_comp_col": True,
        "comp_name": "POR-Primeira Liga"
    },
    {
        "name": "Liga Profesional Argentina",
        "url": 'https://fbref.com/en/comps/21/stats/Liga-Profesional-Argentina-Stats', # Añadido /stats/ si es necesario, FBRef es consistente
        "add_comp_col": True,
        "comp_name": "ARG-Liga Profesional"
    },
    {
        "name": "Serie B Italia",
        "url": 'https://fbref.com/en/comps/18/stats/Serie-B-Stats', # Añadido /stats/ si es necesario
        "add_comp_col": True,
        "comp_name": "ITA-Serie B"
    }
]

all_dataframes = []

print("--- COMENZANDO SCRAPEO DE DATOS DE LIGAS ---")
for league_info in league_data:
    url = league_info["url"]
    print(f"Descargando datos de: {league_info['name']} - {url}")
    try:
        dfs = pd.read_html(url)
        if not dfs:
            print(f"Error: No se encontraron tablas en la URL: {url}")
            continue
        
        df = dfs[0] # Asumimos que la primera tabla es la correcta

        # Eliminar el encabezado de múltiples niveles si existe
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
        
        # Añadir columna 'Comp' si es necesario
        if league_info["add_comp_col"]:
            df['Comp'] = league_info["comp_name"]
            print(f"Añadida columna 'Comp' con valor '{league_info['comp_name']}' para {league_info['name']}")

        # Limpiar filas que contienen el encabezado repetido (donde 'Player' es 'Player')
        if 'Player' in df.columns:
            current_df_cleaned = df[df['Player'] != 'Player'].copy()
        else:
            print(f"Advertencia: Columna 'Player' no encontrada en {url} antes de la limpieza de filas de encabezado. Usando DataFrame como está.")
            current_df_cleaned = df.copy()
        
        all_dataframes.append(current_df_cleaned)
        print(f"Datos de {league_info['name']} procesados. Filas añadidas: {len(current_df_cleaned)}")

    except Exception as e:
        print(f"Error procesando URL {url}: {e}")
        continue

if not all_dataframes:
    print("Error: No se pudieron descargar datos de ninguna liga. El script terminará.")
    exit()

print("\n--- CONCATENANDO TODOS LOS DATAFRAMES ---")
df_cleaned = pd.concat(all_dataframes, ignore_index=True)
print(f"Total de filas después de concatenar todas las ligas: {len(df_cleaned)}")
print(f"Columnas iniciales después de concatenar: {df_cleaned.columns.tolist()}")


# --- COMIENZO DE OPERACIONES DE LIMPIEZA UNIFICADAS ---
print("\n--- COMENZANDO LIMPIEZA DE DATOS UNIFICADA ---")

# 1. Renombrar columnas duplicadas especificadas
print("Renombrando columnas duplicadas (Gls, Ast, etc.)...")
columns_to_rename_duplicates = ['Gls', 'Ast', 'G-PK', 'PK', 'PKatt', 'Sh', 'SoT', 'xG', 'npxG', 'xA', 'npxG+xA', 'G+A', 'xAG', 'npxG+xAG']
for col in columns_to_rename_duplicates:
    if col in df_cleaned.columns:
        # Verificar cuántas veces aparece la columna para decidir si renombrar
        if df_cleaned.columns.tolist().count(col) > 1:
            df_cleaned.columns = rename_duplicates(df_cleaned.columns, col)
            print(f"Columnas '{col}' duplicadas renombradas.")
        else:
            print(f"Columna '{col}' no está duplicada, no se renombra con sufijos _1, _2.")
    else:
        print(f"Advertencia: La columna '{col}' no se encontró para renombrar duplicados.")

# 2. Limpieza adicional de columnas
print("Realizando limpieza adicional de columnas...")
# Crear PlSqu
if 'Player' in df_cleaned.columns and 'Squad' in df_cleaned.columns:
    df_cleaned.loc[:, 'PlSqu'] = df_cleaned['Player'].astype(str) + df_cleaned['Squad'].astype(str)
    print("Columna 'PlSqu' creada.")
else:
    print("Advertencia: Columnas 'Player' o 'Squad' no encontradas para crear 'PlSqu'.")

# Aplicar unidecode
if 'Player' in df_cleaned.columns:
    df_cleaned.loc[:, 'Player'] = df_cleaned['Player'].astype(str).apply(unidecode)
    print("Unidecode aplicado a la columna 'Player'.")
else:
    print("Advertencia: Columna 'Player' no encontrada para aplicar unidecode.")

if 'Squad' in df_cleaned.columns:
    df_cleaned.loc[:, 'Squad'] = df_cleaned['Squad'].astype(str).apply(unidecode)
    print("Unidecode aplicado a la columna 'Squad'.")
else:
    print("Advertencia: Columna 'Squad' no encontrada para aplicar unidecode.")

# Eliminar columna 'Matches' si existe
if 'Matches' in df_cleaned.columns:
    print("Eliminando columna 'Matches'.")
    df_cleaned.drop(columns=['Matches'], inplace=True, errors='ignore') # errors='ignore' por si acaso

# 3. Renombrar columnas finales (ej. para sufijos _p90)
print("Aplicando renombrado final de columnas (sufijos _p90)...")
new_column_names = []
# Columnas que específicamente deben tener _p90 si no son _1 o _2 (es decir, si no fueron duplicadas originalmente)
# Esta lógica asume que si una columna de esta lista no fue duplicada, representa una métrica "per 90"
columns_to_add_p90_suffix_if_single = ['G+A-PK', 'xG+xAG'] 

current_cols = df_cleaned.columns.tolist()
processed_cols = set() # Para llevar la cuenta de columnas ya renombradas

for col_name in current_cols:
    if col_name in processed_cols: # Evitar procesar una columna que ya fue parte de un renombramiento (ej. Gls_1 y Gls_2)
        continue

    original_col_name_if_suffixed = col_name.rsplit('_', 1)[0] if '_' in col_name and col_name.rsplit('_', 1)[1] in ['1', '2'] else col_name

    if f"{original_col_name_if_suffixed}_1" in current_cols and f"{original_col_name_if_suffixed}_2" in current_cols:
        new_column_names.append(original_col_name_if_suffixed) # Para _1
        new_column_names.append(f"{original_col_name_if_suffixed}_p90") # Para _2
        processed_cols.add(f"{original_col_name_if_suffixed}_1")
        processed_cols.add(f"{original_col_name_if_suffixed}_2")
        print(f"Renombrado: '{original_col_name_if_suffixed}_1' -> '{original_col_name_if_suffixed}', '{original_col_name_if_suffixed}_2' -> '{original_col_name_if_suffixed}_p90'")
    elif col_name in columns_to_add_p90_suffix_if_single and not (f"{col_name}_1" in current_cols or f"{col_name}_2" in current_cols):
        # Si la columna está en la lista especial y NO fue duplicada (no tiene _1 o _2)
        new_column_names.append(col_name + '_p90')
        processed_cols.add(col_name)
        print(f"Renombrado especial: '{col_name}' -> '{col_name}_p90'")
    elif not ('_1' in col_name or '_2' in col_name.rsplit('_',1)[-1]): # Evitar añadir columnas ya procesadas o que no deben cambiar
        new_column_names.append(col_name)
        processed_cols.add(col_name)

# Asegurarse de que todas las columnas originales se mapean si no se procesaron arriba
# Esto es un fallback para columnas que no encajan en los patrones de renombrado
final_renamed_columns = []
temp_original_cols_list = df_cleaned.columns.tolist()
renamed_indices = []

# Aplicar los nombres de la lista new_column_names que corresponden a las columnas que SÍ se renombraron
# Esta parte es compleja debido a cómo se reconstruyen las columnas.
# Una forma más simple sería iterar y construir la lista final directamente.

final_new_column_names = []
original_columns_iter = iter(df_cleaned.columns)

# Reconstrucción del renombrado final de columnas:
# La lógica anterior para new_column_names era un poco confusa. Simplificando:
# 1. Columnas con _1 se quita el _1.
# 2. Columnas con _2 se cambia _2 por _p90.
# 3. Columnas en `columns_to_add_p90_suffix_if_single` que no tuvieron _1/_2, se les añade _p90.
# 4. El resto se mantiene.

final_renamed_cols = []
cols_list = df_cleaned.columns.tolist()
temp_map = {} # Para mapear nombres viejos a nuevos temporalmente

# Primero, manejar los pares _1 y _2
base_names_handled_as_pair = set()
for col in cols_list:
    if col.endswith("_1"):
        base_name = col[:-2]
        if f"{base_name}_2" in cols_list:
            temp_map[col] = base_name
            temp_map[f"{base_name}_2"] = f"{base_name}_p90"
            base_names_handled_as_pair.add(base_name)
            print(f"Mapeado par: '{col}' -> '{base_name}', '{base_name}_2' -> '{base_name}_p90'")

# Luego, construir la lista final de nombres
for col in cols_list:
    if col in temp_map:
        final_renamed_cols.append(temp_map[col])
    elif col.endswith("_2") and col[:-2] in base_names_handled_as_pair:
        # Ya fue manejado como parte de un par, no añadir de nuevo
        continue
    elif col in columns_to_add_p90_suffix_if_single and col not in base_names_handled_as_pair:
        # Si es una columna especial y no fue parte de un par _1/_2
        final_renamed_cols.append(f"{col}_p90")
        print(f"Mapeado especial (single): '{col}' -> '{col}_p90'")
    elif not (col.endswith("_1") or col.endswith("_2")): # Columnas que no son _1 ni _2 y no especiales
        final_renamed_cols.append(col)
    # Si es _1 o _2 pero no tiene su par, o no es especial, se podría quedar como está o necesitar otra regla.
    # Por ahora, si no entra en las condiciones anteriores y no está en temp_map, se añade tal cual.
    # Esto podría necesitar ajuste si hay columnas _1 o _2 sin su par.
    elif col not in final_renamed_cols and col not in temp_map.values(): # Fallback
         # Check if it's an orphaned _1 or _2 that wasn't handled
        is_orphan_1 = col.endswith("_1") and col[:-2] not in base_names_handled_as_pair
        is_orphan_2 = col.endswith("_2") and col[:-2] not in base_names_handled_as_pair
        if not (is_orphan_1 or is_orphan_2): # Add if not an unhandled orphan from a pair
             final_renamed_cols.append(col)


# Verificar si la longitud de final_renamed_cols coincide con df_cleaned.columns
if len(final_renamed_cols) == len(df_cleaned.columns):
    df_cleaned.columns = final_renamed_cols
    print("Renombrado final de columnas aplicado.")
else:
    print(f"Error: El número de columnas renombradas ({len(final_renamed_cols)}) no coincide con el original ({len(df_cleaned.columns)}). No se aplicó el renombrado final.")
    print(f"Columnas propuestas para renombrar: {final_renamed_cols}")
    print(f"Columnas originales: {df_cleaned.columns.tolist()}")


# --- FIN DE OPERACIONES DE LIMPIEZA ---

# Crear carpeta de salida si no existe
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)

# Exportar a CSV
output_filename = 'big5_player_stats_standard.csv' # <--- NOMBRE DE ARCHIVO REVERTIDO
output_path = os.path.join(output_dir, output_filename)
df_cleaned.to_csv(output_path, index=False)

print(f"\n--- PROCESO COMPLETADO ---")
print(f"Datos procesados y exportados correctamente a {output_path}")
print(f"Columnas finales: {df_cleaned.columns.tolist()}")
print(f"Primeras 5 filas del DataFrame final:\n{df_cleaned.head()}")
print(f"Últimas 5 filas del DataFrame final:\n{df_cleaned.tail()}")
print(f"Información del DataFrame final:\n")
df_cleaned.info()

