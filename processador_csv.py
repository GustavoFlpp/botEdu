import pandas as pd
from datetime import datetime
import time 
import os
from dotenv import load_dotenv
import gspread 
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

# --- Configuração Sheets/Bot ---
load_dotenv()
SHEET_ID = os.getenv('SHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME')
ARQUIVO_CREDENCIAL = os.getenv('ARQUIVO_CREDENCIAL')

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

# --- Lógica de Conexão e Leitura ---

def conectar_sheets():
    """Conecta ao Google Sheets usando o Service Account e retorna o WorkSheet."""
    print("[LOG] Tentando conectar ao Google Sheets...")
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    try:
        creds = Credentials.from_service_account_file(ARQUIVO_CREDENCIAL, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SHEET_ID)
        print(f"[LOG] Planilha '{spreadsheet.title}' aberta com sucesso.")
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        
        print(f"[LOG] Conexão bem-sucedida! Aba: '{worksheet.title}'")
        return worksheet
    except gspread.exceptions.WorksheetNotFound:
        print(f"--- ERRO FATAL ---")
        print(f"Aba '{SHEET_NAME}' não encontrada na planilha.")
        print(f"Verifique se o nome da aba está escrito EXATAMENTE igual (maiúsculas/minúsculas).")
        raise
    except Exception as e:
        print(f"--- ERRO FATAL ---")
        print(f"Erro de conexão/autenticação com Google Sheets: {e}")
        print("Verifique se o 'credentials.json' está na pasta e se o 'client_email' tem permissão de EDITOR na planilha.")
        raise

def carregar_dataframe(worksheet):
    """
    Lê os dados da aba e constrói o DataFrame com os cabeçalhos corretos.
    (CORREÇÃO 2: Esta função agora corrige o cabeçalho)
    """
    print("[LOG] Carregando todos os dados da aba (get_all_values)...")
    data = worksheet.get_all_values()
    
    if not data or len(data) < 2:
        raise ValueError("Dados insuficientes ou Planilha vazia.")
    
    print(f"[LOG] {len(data)} linhas (brutas) e {len(data[0])} colunas (brutas) lidas.")

    headers_row_1 = data[0] # Cabeçalho Nível 1 (Tarefas)
    headers_row_2 = data[1] # Cabeçalho Nível 2 (Resp, Planejado, Realizado)
    
    novas_colunas = []
    current_header = "" # Armazena o último cabeçalho principal (ex: 'Design Educacional')
    
    print("\n--- LOG DETALHADO: Mapeamento de Colunas (Lógica Corrigida) ---")
    
    for i in range(len(headers_row_2)):
        h1 = headers_row_1[i].strip() # Ex: 'Design Educacional' ou ''
        h2 = headers_row_2[i].strip() # Ex: 'Resp.' ou 'Planejado' ou 'Componente Curricular'

        # Se h1 (linha 1) não estiver vazio, ele é o novo "dono" das colunas
        if h1 and h1 in TAREFAS_PRINCIPAIS:
            current_header = h1
        # Se h1 estiver vazio e h2 for uma coluna principal, reseta o "dono"
        elif not h1 and h2 in ['Entrega', 'Componente Curricular', 'Etapa', 'Eixo', 'Quant.']:
            current_header = ""
            
        # Monta o nome da coluna
        h2_clean = h2.replace('.', '').strip()
        
        if current_header and current_header in TAREFAS_PRINCIPAIS:
            # É uma sub-coluna
            if h2_clean == 'Realizado':
                final_col_name = f"{current_header}_Realizado_Data" # É a data
            elif h2_clean == '':
                final_col_name = f"{current_header}_Status" # É o status TRUE/FALSE
            else:
                final_col_name = f"{current_header}_{h2_clean}"
        else:
            # É uma coluna principal (ex: 'Componente Curricular')
            final_col_name = h2_clean
            
        print(f"[DEBUG] Coluna {i}: (H1: '{h1}', H2: '{h2}') -> Header: '{current_header}' -> Convertida para: {final_col_name}")
        novas_colunas.append(final_col_name)

    print("--- Fim Mapeamento de Colunas ---\n")
            
    df = pd.DataFrame(data[2:], columns=novas_colunas)
    
    # Remove colunas que ficaram com o nome vazio (as colunas de espaçamento)
    df = df.drop(columns=[''], errors='ignore')
    
    # CRÍTICO: Cria o índice real da linha no Sheets. 
    df['indice_linha_sheets'] = df.index + 3 
    
    print(f"[LOG] Colunas do DF prontas. Total: {len(df.columns)} colunas.")
    print(f"[DEBUG] Colunas finais (lista): {df.columns.tolist()}")
    return df

# --- Função de Escrita (Corrigida e Simplificada) ---

def atualizar_status_sheets(row_index, tarefa, novo_status):
    """
    Atualiza uma célula específica na planilha do Google Sheets.
    (CORREÇÃO 9: Agora preenche a data e o status)
    """
    print(f"\n[LOG Sheets] Tentando ATUALIZAR status para '{novo_status}'...")
    worksheet = conectar_sheets()
    
    try:
        print(f"[LOG Sheets] Procurando tarefa '{tarefa}' na Linha 1 do cabeçalho...")
        # 1. Encontra a coluna da Tarefa Principal
        cell = worksheet.find(tarefa, in_row=1)
        
        if not cell:
            print(f"[ERRO Sheets] A tarefa principal '{tarefa}' não foi encontrada na Linha 1 da planilha.")
            raise Exception(f"Tarefa '{tarefa}' não encontrada (Linha 1)")

        col_tarefa = cell.col
        print(f"[LOG Sheets] Tarefa encontrada na Coluna {col_tarefa}. Procurando 'Realizado' na Linha 2...")
        
        # 2. Encontra a subcoluna 'Realizado'
        all_realizado_cells = worksheet.findall('Realizado', in_row=2)
        
        cell_realizado = None
        for cell_r in all_realizado_cells:
            if cell_r.col >= col_tarefa:
                cell_realizado = cell_r
                break 
        
        if not cell_realizado:
            print(f"[ERRO Sheets] Não foi possível encontrar a subcoluna 'Realizado' (Linha 2) correspondente à tarefa '{tarefa}'.")
            raise Exception(f"Subcoluna 'Realizado' não encontrada para '{tarefa}'")

        # 3. DEFINIÇÃO DAS CÉLULAS ALVO (A MUDANÇA ESTÁ AQUI)
        
        # A coluna de DATA é a própria 'Realizado'
        coluna_data_realizado = cell_realizado.col
        # A coluna de STATUS é a seguinte
        coluna_status_final = cell_realizado.col + 1
        
        # Pega a data atual no formato "dd/mm"
        data_hoje = datetime.now().strftime('%d/%m')
        
        print(f"[LOG Sheets] Célula 'Realizado' (Data) encontrada na Coluna {coluna_data_realizado}.")
        print(f"[LOG Sheets] Célula 'Status' encontrada na Coluna {coluna_status_final}.")
        
        # 4. ATUALIZA AMBAS AS CÉLULAS
        
        print(f"[LOG Sheets] Atualizando Célula [Linha {row_index}, Col {coluna_data_realizado}] para '{data_hoje}'...")
        worksheet.update_cell(row_index, coluna_data_realizado, data_hoje)
        
        print(f"[LOG Sheets] Atualizando Célula [Linha {row_index}, Col {coluna_status_final}] para '{novo_status}'...")
        worksheet.update_cell(row_index, coluna_status_final, novo_status)
        
        print("[LOG Sheets] Atualização dupla concluída.")

    except Exception as e:
        print(f"[ERRO Sheets] Falha ao localizar/atualizar a célula. Detalhe: {e}")
        raise


# --- Lógica Principal (encontrar_pendencias) ---

def encontrar_pendencias():
    """
    Conecta, carrega o DF e itera para encontrar as pendências.
    (CORREÇÃO 4: Agora procura pela coluna _Status)
    """
    start_time = time.time()
    print("\n--- INICIANDO VERIFICAÇÃO DE PENDÊNCIAS (Google Sheets) ---")

    try:
        worksheet = conectar_sheets()
        df = carregar_dataframe(worksheet)
        
    except Exception as e:
        print(f"--- ERRO FATAL AO CARREGAR OS DADOS ---")
        print(f"Erro: {e}")
        print("Verificação abortada.")
        return [] 
    
    df = df.dropna(subset=['Componente Curricular'])
    print(f"[LOG] {len(df)} linhas de cursos válidos encontradas.")
    
    pendencias = []
    total_tarefas_checadas = 0
    
    print("\n--- INICIANDO ANÁLISE DETALHADA (Linha por Linha) ---")
    
    for index, row in df.iterrows():
        curso_bruto = row.get('Componente Curricular', '')
        if pd.isna(curso_bruto) or not curso_bruto:
            continue
        curso = str(curso_bruto).strip()
        
        print(f"\n[CURSO] {curso} (Linha Sheets: {row['indice_linha_sheets']})")
        
        for tarefa in TAREFAS_PRINCIPAIS:
            total_tarefas_checadas += 1
            
            # Nomes das colunas corrigidos
            col_resp = f'{tarefa}_Resp'
            col_status = f'{tarefa}_Status' # CORRIGIDO
            col_data = f'{tarefa}_Planejado'
            
            # A verificação agora deve funcionar
            if col_resp not in df.columns or col_data not in df.columns or col_status not in df.columns:
                # print(f"  -> [TAREFA] {tarefa:<30} | IGNORADO (Etapa não se aplica a este curso)")
                continue
                
            pessoa = row.get(col_resp)
            status_bruto = row.get(col_status)
            
            log_prefix = f"  -> [TAREFA] {tarefa:<30}"
            print(f"{log_prefix} | Lendo Resp: '{pessoa}' | Lendo Status: '{status_bruto}'")
            
            pessoa_str = str(pessoa).strip()
            if pd.isna(pessoa) or pessoa_str in ['-', 'FINALIZADO', '']:
                print(f"     -> IGNORADO (Responsável inválido ou 'FINALIZADO')")
                continue
            
            pessoa_limpo = pessoa_str
            status_str = str(status_bruto).upper().strip()
            data_planejada = row.get(col_data, "N/A")

            # Encontrada pendência (Status é 'TRUE')
            if status_str == 'TRUE':
                print(f"     -> PENDÊNCIA ENCONTRADA! (Responsável: {pessoa_limpo})")
                pendencia = {
                    "pessoa": pessoa_limpo,
                    "curso": curso,
                    "tarefa": tarefa.strip(),
                    "dia": data_planejada,
                    "row_index": row['indice_linha_sheets'] 
                }
                pendencias.append(pendencia)
            else:
                print(f"     -> OK (Status: '{status_str}')")

    end_time = time.time()
    print("\n--- VERIFICAÇÃO CONCLUÍDA ---")
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos")
    print(f"Total de tarefas individuais checadas: {total_tarefas_checadas}")
    print(f"Total de pendências encontradas: {len(pendencias)}")
    return pendencias

# --- Para testar este script diretamente ---
if __name__ == "__main__":
    
    lista_de_pendencias = encontrar_pendencias()
    
    if lista_de_pendencias:
        print("\n--- RESUMO DE PENDÊNCIAS (Primeiras 10) ---")
        for item in lista_de_pendencias[:10]:
            print(f"  [PENDENTE] Pessoa: {item['pessoa']}, Curso: {item['curso']}, Tarefa: {item['tarefa']}, Prazo: {item['dia']}, Linha Sheets: {item['row_index']}")
    else:
        print("\n--- RESUMO DE PENDÊNCIAS ---")
        print("  Nenhuma pendência encontrada.")