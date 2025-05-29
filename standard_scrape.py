import pandas as pd
import os

# Leer la tabla de estadísticas desde FBref
url = 'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats'
df = pd.read_html(url)[0]

# Eliminar el encabezado duplicado
df.columns = df.columns.droplevel(0)

# Limpiar filas que contienen el encabezado repetido
df_cleaned = df[df['Player'] != 'Player']

# Crear carpeta de salida si no existe
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)

# Exportar a CSV
output_path = os.path.join(output_dir, 'big5_player_stats.csv')
df_cleaned.to_csv(output_path, index=False)

print(f"Datos exportados correctamente a {output_path}")
