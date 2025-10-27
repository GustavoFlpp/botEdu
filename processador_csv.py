import pandas as pd
from datetime import datetime
import time # Para medir o tempo de execução

# --- Configuração ---

# ATUALIZADO: Nome do arquivo CSV (com base no seu log)
ARQUIVO_CSV = 'planilha_pontoedu.csv' 
# O nome da aba não é necessário para CSV

# Lista das colunas de tarefas principais
TAREFAS_PRINCIPAIS = [
    'Design Educacional',
    'Roteiro (1DIA após DE)',
    'R1 - (3DIAS após DE)',
    'Design Gráfico - (2DIAS após R1)',
    'Beta Tester (2DIAS após DG)',
    'Cotejo (1DIA após Beta)',
    'Diagramação (1DIA após Cotejo)',
    'Publicação AVA',
    'Validação UI & UX (1DIA após publicar)'
]

# --- Lógica Principal ---

def limpar_cabecalhos(df):
    """
    Limpa o cabeçalho duplicado (MultiIndex) do DataFrame.
    Esta versão é mais inteligente para lidar com cabeçalhos principais esparsos.
    """
    print("[LOG] Limpando cabeçalhos duplicados...")
    novas_colunas = []
    current_header = "" # Armazena o último cabeçalho principal (ex: 'Design Educacional')
    
    for col in df.columns:
        col0_str = str(col[0]).strip()
        col1_str = str(col[1]).strip()
        
        # 1. Verifica se encontramos um novo cabeçalho principal
        # Se a coluna superior não for 'Unnamed...', é um novo cabeçalho
        if 'Unnamed' not in col0_str:
            current_header = col0_str
            
        # 2. Constrói o nome da nova coluna
        
        # Se estamos "fora" de um bloco de tarefas (ex: 'Componente Curricular')
        if not current_header:
            novas_colunas.append(col1_str)
        else:
            # Se estamos "dentro" de um bloco (ex: 'Design Educacional')
            
            # Se a coluna inferior for 'Unnamed...', é a coluna de Status
            if 'Unnamed' in col1_str:
                novas_colunas.append(f"{current_header}_Status")
            else:
                # É uma coluna normal como 'Resp.', 'Planejado', etc.
                novas_colunas.append(f"{current_header}_{col1_str}")
            
    df.columns = novas_colunas
    # Este log de exemplo agora deve mostrar os nomes corretos
    print(f"[LOG] Cabeçalhos limpos. Colunas exemplo (índices 5-8): {df.columns[5:9].tolist()}")

    # --- NOVO LOG DE DEBUG ---
    print("\n[DEBUG] Lista completa de colunas geradas:")
    print(df.columns.tolist())
    print("--- FIM DEBUG COLUNAS ---\n")
    # --- FIM NOVO LOG ---
    
    # --- PONTO DE PAUSA REMOVIDO ---
    # input(">>> [PAUSA] Copie a lista de colunas acima (do '[' ao ']') e cole no chat. Pressione ENTER para continuar a verificação...")

    return df

def encontrar_pendencias():
    """
    Lê o arquivo CSV e encontra todas as tarefas pendentes,
    imprimindo logs detalhados do processo.
    """
    start_time = time.time()
    print(f"\n--- INICIANDO VERIFICAÇÃO DE PENDÊNCIAS ({time.strftime('%Y-%m-%d %H:%M:%S')}) ---")
    print(f"[LOG] Tentando ler arquivo: '{ARQUIVO_CSV}'")
    
    try:
        # --- ATUALIZADO: Voltamos para pd.read_csv ---
        df = pd.read_csv(
            ARQUIVO_CSV, 
            header=[0, 1] # As duas primeiras linhas são cabeçalhos
        )
        print(f"[LOG] Arquivo CSV lido com sucesso.")
        
    except FileNotFoundError:
        print(f"[ERRO] Arquivo '{ARQUIVO_CSV}' não encontrado.")
        print("       Por favor, coloque o arquivo CSV no mesmo diretório.")
        return []
    except Exception as e:
        # Captura outros erros, como problemas de encoding ou cabeçalho
        print(f"[ERRO] Erro ao ler o arquivo CSV: {e}")
        print(f"       Verifique se o arquivo não está corrompido.")
        return []

    df = limpar_cabecalhos(df)
    
    # Remove linhas que não são de dados (ex: linhas em branco ou de subtotal)
    df = df.dropna(subset=['Componente Curricular'])
    print(f"[LOG] {len(df)} linhas de cursos válidos encontradas.")
    
    pendencias = []
    total_tarefas_checadas = 0
    
    print("\n--- INICIANDO ANÁLISE DETALHADA ---")
    
    # Itera por cada linha (cada curso)
    for index, row in df.iterrows():
        # Trata cursos que podem não ser strings
        curso_bruto = row['Componente Curricular']
        if pd.isna(curso_bruto):
            print(f"\n[CURSO] IGNORADO (Componente Curricular vazio na Linha {index + 3})")
            continue
        curso = str(curso_bruto).strip()
        
        print(f"\n[CURSO] Processando: '{curso}' (Linha {index + 3})") # +3 para contar cabeçalhos e index 0
        
        # Itera por cada etapa da tarefa (Design, Roteiro, etc.)
        for tarefa in TAREFAS_PRINCIPAIS:
            total_tarefas_checadas += 1
            col_resp = f'{tarefa}_Resp.'
            col_status = f'{tarefa}_Status'
            col_data = f'{tarefa}_Planejado'
            
            # Log inicial da tarefa
            log_prefix = f"  -> [TAREFA] {tarefa}:"
            
            # --- LÓGICA DE VERIFICAÇÃO ATUALIZADA ---
            
            # 1. Verifica se as colunas BÁSICAS existem (Resp e Planejado)
            if col_resp not in df.columns or col_data not in df.columns:
                print(f"{log_prefix} AVISO: Colunas básicas não encontradas. (Esperando: '{col_resp}', '{col_data}'). Pulando esta tarefa.")
                continue
                
            # 2. Verifica se a coluna de STATUS existe. Se não, não podemos checar pendência.
            if col_status not in df.columns:
                print(f"{log_prefix} AVISO: Coluna de Status não encontrada. (Esperando: '{col_status}'). Não é possível checar pendência para esta tarefa.")
                continue
            
            # --- FIM DA ATUALIZAÇÃO ---
                
            pessoa = row[col_resp]
            status_bruto = row[col_status]
            status_str = str(status_bruto).upper().strip()
            data_planejada = row[col_data]
            
            # --- NOVO LOG DE DEBUG ---
            # Imprime os valores brutos lidos antes de qualquer lógica
            print(f"{log_prefix} Lendo valores... (Pessoa: '{pessoa}', Status: '{status_bruto}', Data: '{data_planejada}')")
            # --- FIM NOVO LOG ---

            # --- LÓGICA DE VALIDAÇÃO COM LOGS ---
            
            # 1. Verifica se há um responsável
            if pd.isna(pessoa) or pessoa.strip() == '-' or pessoa.strip() == 'FINALIZADO' or pessoa.strip() == '':
                print(f"{log_prefix} IGNORADO (Sem responsável válido: '{pessoa}')")
                continue
            
            pessoa_limpo = pessoa.strip()
            
            # 2. Verifica o status
            # TRUE = Pendente (como você mencionou)
            if status_str == 'TRUE':
                print(f"{log_prefix} PENDÊNCIA ENCONTRADA! (Responsável: {pessoa_limpo}, Status: {status_bruto})")
                pendencia = {
                    "pessoa": pessoa_limpo,
                    "curso": curso,
                    "tarefa": tarefa.strip(),
                    "dia": data_planejada
                }
                pendencias.append(pendencia)
            
            # 3. Se for 'FALSE' (concluído)
            elif status_str == 'FALSE':
                print(f"{log_prefix} OK (Responsável: {pessoa_limpo}, Status: {status_bruto})")
            
            # 4. Outros status (ex: 'NAN', 'NONE', em branco)
            else:
                 # --- LOG ATUALIZADO ---
                 print(f"{log_prefix} IGNORADO (Status não é TRUE nem FALSE. Responsável: {pessoa_limpo}, Status: {status_bruto})")

    end_time = time.time()
    print("\n--- VERIFICAÇÃO CONCLUÍDA ---")
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
    print(f"Total de linhas de cursos analisadas: {len(df)}")
    print(f"Total de tarefas individuais checadas: {total_tarefas_checadas}")
    print(f"Total de pendências encontradas: {len(pendencias)}")
    return pendencias

# --- Para testar este script diretamente ---
if __name__ == "__main__":
    # Certifique-se de ter o arquivo 'planilha_pontoedu.csv' no diretório
    # e de ter rodado: pip install pandas
    
    lista_de_pendencias = encontrar_pendencias()
    
    if lista_de_pendencias:
        print("\n--- RESUMO DE PENDÊNCIAS ---")
        for item in lista_de_pendencias:
            print(f"  [PENDENTE] Pessoa: {item['pessoa']}, Curso: {item['curso']}, Tarefa: {item['tarefa']}, Prazo: {item['dia']}")
    else:
        print("\n--- RESUMO DE PENDÊNCIAS ---")
        print("  Nenhuma pendência encontrada.")

