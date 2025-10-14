import psycopg2
import psutil
import os
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO ---
# Altere estas variáveis para corresponder ao seu ambiente

# Configuração do Banco de Dados PostgreSQL do dcm4chee
DB_CONFIG = {
    "dbname": "pacsdb",      # Nome do banco de dados do dcm4chee
    "user": "postgres",      # Usuário do banco de dados
    "password": "roberto",   # Senha do usuário
    "host": "10.2.0.10",     # Endereço do servidor de banco de dados
    "port": "5432"           # Porta do PostgreSQL
}

# Caminho para o diretório de armazenamento (repositório) do PACS
# Exemplo Windows: "D:\\pacs\\storage"
# Exemplo Linux: "/var/local/dcm4chee/archive"
REPOSITORY_PATH = "C:\\"

# Número de dias no passado para calcular a média de uso
# 30 dias é um bom ponto de partida para suavizar picos de uso
DAYS_TO_AVERAGE = 30

# --- FIM DA CONFIGURAÇÃO ---


def format_bytes(byte_count):
    """Formata um valor em bytes para uma string legível (KB, MB, GB, TB)."""
    if byte_count is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while byte_count >= power and n < len(power_labels) -1 :
        byte_count /= power
        n += 1
    return f"{byte_count:.2f} {power_labels[n]}B"

def get_free_space_bytes(path):
    """
    Retorna o espaço livre em bytes para um dado caminho.
    Retorna None se o caminho não existir.
    """
    if not os.path.exists(path):
        print(f"ERRO: O caminho do repositório '{path}' não foi encontrado.")
        return None
    try:
        disk_usage = psutil.disk_usage(path)
        return disk_usage.free
    except Exception as e:
        print(f"ERRO: Não foi possível verificar o espaço em disco em '{path}': {e}")
        return None

def get_average_daily_usage_bytes(db_params, days):
    """
    Conecta ao banco de dados do dcm4chee e calcula a média de bytes
    arquivados por dia nos últimos 'days' dias.
    """
    conn = None
    avg_bytes = None
    

    query = """
    WITH daily_usage AS (
    SELECT
        CAST(s.created_time AS DATE) AS archive_date,
        SUM(f.file_size) AS total_bytes_per_day
    FROM study s
    JOIN series se ON s.pk = se.study_fk
    JOIN instance i ON se.pk = i.series_fk
    JOIN files f ON i.pk = f.instance_fk
    WHERE s.created_time >= %s
    GROUP BY archive_date
    )
    SELECT AVG(total_bytes_per_day)
     FROM daily_usage;
    """
    
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Define a data de início para a consulta
        start_date = datetime.now() - timedelta(days=days)
        
        cur.execute(query, (start_date,))
        result = cur.fetchone()
        
        if result and result[0] is not None:
            avg_bytes = float(result[0])
            
        cur.close()
    except psycopg2.OperationalError as e:
        print(f"ERRO: Falha ao conectar ao banco de dados: {e}")
        return None
    except psycopg2.errors.UndefinedTable as e:
         print(f"ERRO: Tabela não encontrada. A query pode não ser compatível com sua versão do dcm4chee. {e}")
         return None
    except Exception as e:
        print(f"ERRO: Ocorreu um erro ao consultar o banco de dados: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()
            
    return avg_bytes

def main():
    """Função principal que executa a previsão."""
    print("--- Previsão de Armazenamento do PACS ---")
    
    # 1. Obter espaço livre
    free_space = get_free_space_bytes(REPOSITORY_PATH)
    if free_space is None:
        return  # Encerra se não conseguir obter o espaço em disco
        
    print(f"Espaço livre no repositório: {format_bytes(free_space)}")
    
    # 2. Calcular média de uso
    print(f"Calculando a média de uso diário dos últimos {DAYS_TO_AVERAGE} dias...")
    avg_usage = get_average_daily_usage_bytes(DB_CONFIG, DAYS_TO_AVERAGE)
    
    if avg_usage is None:
        print("Não foi possível calcular a média de uso. A previsão não pode ser gerada.")
        return
        
    if avg_usage == 0:
        print("Média de uso diário é zero. Não há dados de arquivamento no período selecionado.")
        print("Previsão: O armazenamento durará indefinidamente nas condições atuais.")
        return
        
    print(f"Média de arquivamento diário: {format_bytes(avg_usage)}")
    
    # 3. Calcular a previsão
    days_remaining = free_space / avg_usage
    
    print("\n--- RESULTADO DA PREVISÃO ---")
    print(f"Com base na média de uso, o espaço de armazenamento restante é suficiente para aproximadamente:")
    print(f" >> {days_remaining:.0f} dias <<")
    
    # Calcula a data estimada
    estimated_date = datetime.now() + timedelta(days=days_remaining)
    print(f"Data estimada para esgotamento do espaço: {estimated_date.strftime('%d de %B de %Y')}")
    print("---------------------------------")


if __name__ == "__main__":
    main()
