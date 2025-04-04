# Importar a biblioteca DuckDB para manipulação de dados
import os
import glob
import duckdb
import pandas as pd

# Cria variavel para conexão do banco de dados
global con
con = duckdb.connect(database=':memory:')
os.system('rm -rf .tmp')

# Função para criar a tabela no banco de dados e inicializa todas as variáveis vazias
def criar_base(arquivo, nome_base, variaveis):
    # Verifica se o arquivo existe, se sim carrega o arquivo
    if os.path.exists(arquivo):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {nome_base} AS SELECT * FROM read_csv_auto('{arquivo}')
        """)
    else:
        # Criar a string com os nomes das colunas e os tipos corretos
        columns_def = ', '.join(
            f"{col} VARCHAR" if col == "acronym" else
            f"{col} TIMESTAMP" if col == "timestamp" else
            f"{col} INT" if col in ["year", "day", "min"] else
            f"{col} FLOAT" 
            for col in variaveis        )
        
        # Criar a tabela
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {nome_base} ({columns_def})
        """)

# Função para popular a tabela com os dados, nessa função será possível fazer o append dos dados, atualizar os dados variavel a variavel
def popular_base(args):
    arquivo, base, linha_linha, var_var, sobreescrever = args

    # Le o arquivo CSV e insere os dados na tabela correspondente
    try:
        # Lendo os dados do CSV, pulando a segunda linha que contém informações extras
        new_data = pd.read_csv(arquivo, skiprows=[1])
        # Coluna de variáveis (todas as colunas após as primeiras 5 que são metadados)
        variaveis = new_data.columns[5:]
        # Limpeza de dados: substituir vírgulas por pontos em dados numéricos
        new_data[variaveis] = new_data[variaveis].replace({',': '.'}, regex=True)
        # Substituir valores como "xxx-" por 0 (dados inválidos)
        new_data[variaveis] = new_data[variaveis].replace({r'\d+-': '0'}, regex=True)
        # Converter todos os dados para numérico quando possível
        new_data[variaveis] = new_data[variaveis].apply(pd.to_numeric, errors='coerce')
        # Formatar corretamente o timestamp
        new_data['timestamp'] = pd.to_datetime(new_data['timestamp'], errors='coerce')
        # Verificar se há dados inválidos na coluna timestamp
        invalid_timestamps = new_data['timestamp'].isna().sum()
        if invalid_timestamps > 0:
            print(f"Aviso: {invalid_timestamps} timestamps inválidos encontrados no arquivo {arquivo}")
    
        # Verificar se o acronimo é válido
        if 'acronym' not in new_data.columns or new_data['acronym'].isna().all():
            print(f"Erro: Arquivo {arquivo} não possui informação de estação (acronym)")
            return
        estacao = new_data['acronym'].iloc[0]
        # Pega o intervalo de tempo do arquivo
        tempo = (new_data['timestamp'].min(), new_data['timestamp'].max())

        # Obter as colunas que existem no DataFrame
        df_columns = list(new_data.columns)
        columns_str = ', '.join(df_columns)

        # Verifica se as colunas do DataFrame estão na tabela, excluindo as que não existem
        table_columns = con.execute(f"DESCRIBE {base}").fetchall()
        table_columns = [col[0] for col in table_columns]
        columns_to_insert = [col for col in df_columns if col in table_columns]
        columns_str = ', '.join(columns_to_insert)

        # Separa apenas as colunas que existem na tabela
        new_data = new_data[columns_to_insert]        
        
        # Registra o DataFrame no DuckDB para operações
        con.register('new_data', new_data)

        # Se for para sobreescrever, exclui os dados existentes
        if sobreescrever:
            con.execute(f"DELETE FROM {base} WHERE acronym = '{estacao}' AND timestamp BETWEEN '{tempo[0]}' AND '{tempo[1]}'")
        # Inserção linha a linha e variável a variável (modo mais detalhado)
        if linha_linha:
            for i in range(len(new_data)):
                row = new_data.iloc[i]
                # Modo variável a variável
                if var_var:
                    for v in variaveis:
                        # Verifica se o registro já existe na base
                        if not sobreescrever:
                            exists = con.execute(
                                f"SELECT COUNT(*) FROM {base} WHERE acronym = '{estacao}' AND timestamp = '{row['timestamp']}' AND {v} = {row[v]}"
                            ).fetchone()[0]
                            if exists > 0:
                                continue
                                
                        # Inserção de dados variável por variável
                        try:
                            con.execute(
                                f"INSERT INTO {base} (acronym, timestamp, year, day, min, {v}) VALUES ('{estacao}', '{row['timestamp']}', {row['year']}, {row['day']}, {row['min']}, {row[v]})"
                            )
                        except Exception as e:
                            print(f"Erro ao inserir variável {v} na base {base}: {e}")
                else:
                    # Verifica se o registro linha a linha já existe
                    if not sobreescrever:
                        exists = con.execute(
                            f"SELECT COUNT(*) FROM {base} WHERE acronym = '{estacao}' AND timestamp = '{row['timestamp']}'"
                        ).fetchone()[0]
                        
                        if exists > 0:
                            continue
                            
                    # Inserção de dados linha a linha
                    try:
                        con.execute(f"INSERT INTO {base} SELECT {columns_str} FROM new_data WHERE timestamp = '{row['timestamp']}'")
                    except Exception as e:
                        print(f"Erro ao inserir linha na base {base}: {e}")
        else:
            # Verificação de dados existentes para inserção em lote
            if not sobreescrever:
                count = con.execute(
                    f"SELECT COUNT(*) FROM {base} WHERE acronym = '{estacao}' AND timestamp BETWEEN '{tempo[0]}' AND '{tempo[1]}'"
                ).fetchone()[0]
                
                if count > 0:
                    print(f"Dados já existem para estação {estacao} no período {tempo[0]} a {tempo[1]}. Use sobreescrever=True para substituir.")
                    return
            
            # Inserir todos os dados de uma vez
            try:
                # Inserir apenas as colunas disponíveis
                con.execute(f"INSERT INTO {base} ({columns_str}) SELECT {columns_str} FROM new_data")
                print(f"Inseridos {len(new_data)} registros na base {base} para estação {estacao}")
            except Exception as e:
                print(f"Erro ao inserir dados na base {base} do arquivo {arquivo}: {e}")
    
        

    except Exception as e:
        print(f"Erro ao processar arquivo {arquivo}: {e}")

# Função para processar os arquivos em paralelo
def processar_arquivos(arquivos, base, linha_linha, var_var, sobreescrever):
    print(f"Processando {len(arquivos)} arquivos para a base {base}...")
    
    # Cria uma lista de argumentos para cada arquivo
    args = [(arquivo, base, linha_linha, var_var, sobreescrever) for arquivo in arquivos]
    
    # Processa os arquivos em serie
    for arg in args:
        popular_base(arg)
    
    # Verificar quantos registros foram inseridos
    count = con.execute(f"SELECT COUNT(*) FROM {base}").fetchone()[0]
    print(f"Total de registros na base {base} após processamento: {count}")


if __name__ == "__main__":

    ############ DECLARAÇÃO DE VARIÁVEIS ############
    # Diretório onde os arquivos estão localizados
    DIRETORIO = '/media/helvecioneto/Barracuda/sonda-formatados/'

    # Apontar o caminho das bases de dados
    ARQV_METEOROLOGICO = '/media/helvecioneto/Barracuda/sonda/dados_meteorologicos.parquet'
    ARQV_SOLARIMETRICA = '/media/helvecioneto/Barracuda/sonda/dados_solarimetricos.parquet'
    ARQV_ANEMOMETRICO = '/media/helvecioneto/Barracuda/sonda/dados_anemometricos.parquet'

    # Nome das tabelas
    BASE_METEOROLOGICO = 'base_meteorologica'
    BASE_SOLARIMETRICA = 'base_solarimetrica'
    BASE_ANEMOMETRICO = 'base_anemometrica'

    # Variaveis
    SOLAR_VAR = "acronym","timestamp","year","day","min","glo_avg","glo_std","glo_max","glo_min","dif_avg","dif_std","dif_max","dif_min","par_avg","par_std","par_max","par_min","lux_avg","lux_std","lux_max","lux_min","dir_avg","dir_std","dir_max","dir_min","lw_calc_avg","lw_calc_std","lw_calc_max","lw_calc_min","lw_raw_avg","lw_raw_std","lw_raw_max","lw_raw_min","tp_glo","tp_dir","tp_dif","tp_lw_dome","tp_lw_case"

    METEO_VAR = "acronym","timestamp","year","day","min","tp_sfc","humid_sfc","press","rain","ws10_avg","ws10_std","wd10_avg","wd10_std"

    ANEMO_VAR = "acronym","timestamp","year","day","min","ws10_avg","ws10_std","ws10_min","ws10_max","wd10_avg","wd10_std","ws25_avg","ws25_std","ws25_min","ws25_max","wd25_avg","wd25_std","tp_25","ws50_avg","ws50_std","ws50_min","ws50_max","wd50_avg","wd50_std","tp_50"


    ############### CRIAÇÃO DAS BASES DE DADOS ##############
    # Criar a base de dados meteorológicos
    criar_base(ARQV_METEOROLOGICO, BASE_METEOROLOGICO, METEO_VAR)
    # Criar a base de dados solarimétricos
    criar_base(ARQV_SOLARIMETRICA, BASE_SOLARIMETRICA, SOLAR_VAR)
    # Criar a base de dados anemométricos
    criar_base(ARQV_ANEMOMETRICO, BASE_ANEMOMETRICO, ANEMO_VAR)

    # listar todos os dados de Solarimétricos usando o glob só para o tipo de arquivo .csv
    # Remove arquivos que contenham 'YYYY_MM_MD_DQC'
    dados_metereologicos = glob.glob(DIRETORIO + "*/Meteorologicos/**/*.csv", recursive=True)
    dados_metereologicos = [arquivo for arquivo in dados_metereologicos if 'YYYY_MM' not in arquivo]
    dados_solarimetricos = glob.glob(DIRETORIO + "*/Solarimetricos/**/*.csv", recursive=True)
    dados_solarimetricos = [arquivo for arquivo in dados_solarimetricos if 'YYYY_MM' not in arquivo]
    dados_anemometricos = glob.glob(DIRETORIO + "*/Anemometricos/**/*.csv", recursive=True)
    dados_anemometricos = [arquivo for arquivo in dados_anemometricos if 'YYYY_MM' not in arquivo]

    # Processar os arquivos de dados meteorológicos
    processar_arquivos(dados_metereologicos, BASE_METEOROLOGICO, linha_linha=False, var_var=False, sobreescrever=False)
    # Processar os arquivos de dados solarimétricos
    processar_arquivos(dados_solarimetricos, BASE_SOLARIMETRICA, linha_linha=False, var_var=False, sobreescrever=False)
    # Processar os arquivos de dados anemométricos
    processar_arquivos(dados_anemometricos, BASE_ANEMOMETRICO, linha_linha=False, var_var=False, sobreescrever=False)

    # Verificar se as tabelas têm dados antes de salvar
    count_meteorologico = con.execute(f"SELECT COUNT(*) FROM {BASE_METEOROLOGICO}").fetchone()[0]
    count_solarimetrico = con.execute(f"SELECT COUNT(*) FROM {BASE_SOLARIMETRICA}").fetchone()[0]
    count_anemometrico = con.execute(f"SELECT COUNT(*) FROM {BASE_ANEMOMETRICO}").fetchone()[0]
    
    print(f"Registros na base meteorológica: {count_meteorologico}")
    print(f"Registros na base solarimétrica: {count_solarimetrico}")
    print(f"Registros na base anemométrica: {count_anemometrico}")
    
    # Salvar a base de dados meteorológicos em um arquivo parquet
    if count_meteorologico > 0:
        con.execute(f"COPY {BASE_METEOROLOGICO} TO '{ARQV_METEOROLOGICO}' (FORMAT 'parquet')")
        print(f"Base de dados meteorológicos salva em {ARQV_METEOROLOGICO}")
    else:
        print(f"A base {BASE_METEOROLOGICO} está vazia, nenhum arquivo foi salvo.")
        
    # Salvar a base de dados solarimétricos em um arquivo parquet
    if count_solarimetrico > 0:
        con.execute(f"COPY {BASE_SOLARIMETRICA} TO '{ARQV_SOLARIMETRICA}' (FORMAT 'parquet')")
        print(f"Base de dados solarimétricos salva em {ARQV_SOLARIMETRICA}")
    else:
        print(f"A base {BASE_SOLARIMETRICA} está vazia, nenhum arquivo foi salvo.")
        
    # Salvar a base de dados anemométricos em um arquivo parquet
    if count_anemometrico > 0:
        con.execute(f"COPY {BASE_ANEMOMETRICO} TO '{ARQV_ANEMOMETRICO}' (FORMAT 'parquet')")
        print(f"Base de dados anemométricos salva em {ARQV_ANEMOMETRICO}")
    else:
        print(f"A base {BASE_ANEMOMETRICO} está vazia, nenhum arquivo foi salvo.")