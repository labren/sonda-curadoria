import duckdb
import pandas as pd
import pathlib
import os
import logging

ARQUIVO_PARQUET = 'Solarimetrica.parquet'
ARQUIVO_ESTACOES = 'Tabela-estacao.csv'
OUTPUT_WEB = 'output_web'

# Configuração do logging error
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Create database in file mode
con = duckdb.connect()

columns_of_interest = [
    'acronym', 'timestamp', 'year', 'day', 'min', 
    'glo_avg', 'dir_avg', 'dif_avg', 'lw_avg', 'par_avg', 'lux_avg'
]


# Create a table with only the columns of interest
con.execute(f"""CREATE TABLE IF NOT EXISTS solarimetrica AS 
            SELECT {', '.join(columns_of_interest)} FROM read_parquet('{ARQUIVO_PARQUET}')""")

# Obter lista de estações únicas
estacoes_query = "SELECT DISTINCT acronym FROM solarimetrica ORDER BY acronym"
estacoes = con.execute(estacoes_query).fetchall()
estacoes = [row for row in estacoes if row[0] is not None]

print(f"Encontradas {len(estacoes)} estações:")


# Ler arquivo Tabela-estados.csv
estacoes_df = pd.read_csv(ARQUIVO_ESTACOES)

# Replace , with . in the 'lat', 'lon', and 'alt' columns
# estacoes_df['Latitude'] = estacoes_df['Latitude'].str.replace(',', '.')
# estacoes_df['Longitude'] = estacoes_df['Longitude'].str.replace(',', '.')
# estacoes_df['Altitude'] = estacoes_df['Altitude'].str.replace(',', '.')
# estacoes_df['Latitude'] = estacoes_df['Latitude'].astype(float)
# estacoes_df['Longitude'] = estacoes_df['Longitude'].astype(float)
# estacoes_df['Alt.(m)'] = estacoes_df['Alt.(m)'].astype(float)

# Loop por cada estação
for estacao_row in estacoes:
    acronym = estacao_row[0]
    print(f"Processando estação: {acronym}")

    # Pega o nome da estação de estacoes_df
    estacao_info = estacoes_df[estacoes_df['Sigla'] == acronym]
    if estacao_info.empty:
        nome_estacao = 'Desconhecida'
        lat_estacao = 'Desconhecida'
        lon_estacao = 'Desconhecida'
        alt_estacao = 'Desconhecida'
        logging.error(f"Informações da estação {acronym}, {estacao_row} não encontradas no arquivo {ARQUIVO_ESTACOES}.")
    else:
        nome_estacao = estacao_info['Sigla'].values[0]
        lat_estacao = estacao_info['Latitude'].values[0]
        lon_estacao = estacao_info['Longitude'].values[0]
        alt_estacao = estacao_info['Alt.(m)'].values[0]

    # Criar MultiIndex com os nomes das colunas e unidades
    multi_columns = pd.MultiIndex.from_arrays([
    [acronym,nome_estacao,'lat:'+str(lat_estacao),'lon:'+str(lon_estacao),'alt:'+str(alt_estacao),'SONDA Network','http://sonda.ccst.inpe.br','sonda@inpe.br', '','',''],
    columns_of_interest,
    ['','','','','','W/m2','W/m2','W/m2','W/m2','µmols/m2.s','klux']
    ])


    # Query para obter os dados da estação
    query = f"""
    SELECT {', '.join(columns_of_interest)}
    FROM solarimetrica 
    WHERE acronym = '{acronym}'
    ORDER BY timestamp
    """
    df = con.execute(query).df()

    # Agrupar por ano baseado na coluna 'year'
    for year in df['year'].unique():
        df_year = df[df['year'] == year]
        df_year.columns = multi_columns
        output_file = pathlib.Path(OUTPUT_WEB) / f"anual/Solarimetrico/{acronym}/{year}/{acronym}_{year}_SD"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_year.to_csv(output_file.with_suffix('.dat'), index=False)
        os.system(f"zip -j {output_file}.zip {output_file.with_suffix('.dat')}")
        os.remove(output_file.with_suffix('.dat'))

    # Agrupa por ano e mês
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df_year_month = df.groupby([df['timestamp'].dt.year, df['timestamp'].dt.month])
    for (year, month), group in df_year_month:
        group.columns = multi_columns
        output_file = pathlib.Path(OUTPUT_WEB) / f"mensal/Solarimetrico/{acronym}/{year}/{acronym}_{year}_{str(month).zfill(2)}_SD"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        group.to_csv(output_file.with_suffix('.dat'), index=False)
        os.system(f"zip -j {output_file}.zip {output_file.with_suffix('.dat')}")
        os.remove(output_file.with_suffix('.dat'))


print("Processamento concluído.")
con.close()



