import discord
from discord.ext import tasks
import os
from dotenv import load_dotenv
import asyncio
from datetime import time, datetime
# Importa as funções de leitura e escrita do Sheets
from processador_csv import encontrar_pendencias, atualizar_status_sheets 

# --- Configuração ---
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
LOG_CHANNEL_ID = int(os.getenv('LOG_CHANNEL_ID')) 
GUILD_ID = int(os.getenv('GUILD_ID'))

USER_MAP = {
    "Gustavo Felippe": 533170999990943744,
    "Gustavo Vieira": 1333794382511345685,
    # "Liliene": 1430517863814266932,
    # "Tamires": 1430891026028953742,
    # "Vicente": 1430899113070694580,
    # "Flavio": 1430243149220937789,
    # "Jônatas": 1430242660882452481,
    # "Amanda": 1430229973586940014,
    # "Stephanie": 1430246757786325164,
    # "Nilce": 1430229845379649558,
    # "Willy": 1430231162214682625,
    # "Thiago": 1430219344310440110,
    # "Gabriela": [1430230600622801029, 1431264918170112112],
    # "Laura": 1314641154050101339,
    # "Everton": 1311060561706090641,
    # "Leandro": 1431290129712156845,
    # "Viviane": [1342689810686541854, 1430522936808177708, 1430685951541121110], 
}

# --- Horários para o bot rodar (fuso horário UTC por padrão) ---
SCHEDULED_TIMES = [
    time(12, 0),  # 09:00 BRT
    time(21, 0),  # 18:00 BRT
]

def criar_mensagem_pendencia(pessoa, curso, tarefa, dia):
    """Cria a mensagem de cobrança inicial para DM."""
    mention = f"**{pessoa}**"
    
    return (
        f"Olá {mention}! 👋\n"
        f"Notei que a tarefa **'{tarefa}'** do curso **'{curso}'** estava planejada para **{dia}**.\n"
        f"Você já finalizou?"
    )

MSG_PARABENS = (
    "Parabéns! 🥳 Ótimo trabalho, já atualizei aqui."
    " (A atualização na planilha foi enviada, mas verifique o canal de logs em caso de erro!)"
)

MSG_ENCORAJAMENTO = (
    "Entendido! 💪 Sem problemas. Lembre-se da importância dessa entrega para o cronograma. "
    "Qualquer dificuldade, avise a equipe!"
)

class TaskView(discord.ui.View):
    """Cria os botões 'Sim' e 'Não'."""
    def __init__(self, pendencia):
        super().__init__(timeout=86400)
        self.pendencia = pendencia

    @discord.ui.button(label="Sim, finalizei!", style=discord.ButtonStyle.success)
    async def sim_callback(self, button: discord.ui.Button, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, invisible=False)
        
        # --- Lógica para atualizar o Google Sheets ---
        pendencia = self.pendencia
        log_channel = interaction.client.get_channel(LOG_CHANNEL_ID)
        
        try:
            row_index = pendencia.get("row_index")
            tarefa = pendencia.get("tarefa")
            
            # Chama a função de escrita, que se conecta ao Sheets e faz a alteração.
            atualizar_status_sheets(
                row_index=row_index, 
                tarefa=tarefa, 
                novo_status='FALSE' # Seta o status para concluído
            )
            
            # Loga o sucesso no canal do Discord
            if log_channel:
                 await log_channel.send(f"✅ Tarefa concluída registrada: **{tarefa}** de **{pendencia.get('curso')}** por **{pendencia.get('pessoa')}**.")
            
        except Exception as e:
            # Loga o erro no console e no canal do Discord
            print(f"[ERRO Sheets] Falha ao atualizar planilha: {e}")
            if log_channel:
                 await log_channel.send(f"⚠️ **ERRO DE ATUALIZAÇÃO SHEETS** para {pendencia.get('pessoa')} na tarefa **{pendencia.get('tarefa')}**: `{e}`")
            # Envia mensagem de erro ao usuário (ephemeral)
            await interaction.followup.send(f"⚠️ Erro ao registrar a conclusão na planilha! Avise o administrador. (Detalhe: `{e}`)", ephemeral=True)
            return # Sai da função para que os botões não sejam desabilitados se a gravação falhou.
            
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        
        await interaction.followup.send(MSG_PARABENS, ephemeral=True)

    @discord.ui.button(label="Ainda não", style=discord.ButtonStyle.danger)
    async def nao_callback(self, button: discord.ui.Button, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, invisible=False)
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        await interaction.followup.send(MSG_ENCORAJAMENTO, ephemeral=True)

intents = discord.Intents.default()
bot = discord.Bot(intents=intents, auto_sync_commands=False) 

@bot.event
async def on_ready():
    print(f"Bot conectado como {bot.user}")
    try:
        print("Sincronizando comandos com o servidor...")
        await bot.sync_commands(guild_ids=[GUILD_ID])
        print("Comandos sincronizados.")
    except discord.errors.Forbidden:
        print("--- ERRO DE PERMISSÃO (50001) ---")
        print("Verifique as permissões 'bot' e 'applications.commands' ao convidar.")
    except Exception as e:
        print(f"ERRO ao sincronizar comandos: {e}")
    # print("Iniciando a tarefa agendada...") # Desabilitado para teste
    # run_daily_check.start() # Desabilitado para teste
    print("Bot pronto. Use o comando /verificar para teste manual.")

async def verificar_pendencias():
    """Função principal que busca pendências e envia DMs."""
    print(f"\n[{datetime.now()}] --- RODANDO VERIFICAÇÃO DE PENDÊNCIAS ---")
    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if not log_channel:
        print(f"[ERRO CRÍTICO] Não foi possível encontrar o CANAL DE LOG com ID: {LOG_CHANNEL_ID}. Logs de erro não serão enviados.")
    
    sucessos_dm = 0
    falhas_dm = 0
    erros_map = []
    pendencias_total = 0

    try:
        pendencias = encontrar_pendencias()
        pendencias_total = len(pendencias)
        
        if not pendencias:
            print("Nenhuma pendência encontrada.")
            if log_channel:
                await log_channel.send("✅ Verificação concluída. Nenhuma pendência encontrada!")
            return
            
        print(f"Encontradas {pendencias_total} pendências. Tentando enviar DMs...")
        
        for p in pendencias:
            nome_pessoa = p.get("pessoa")
            user_id = USER_MAP.get(nome_pessoa)
            
            if not user_id:
                if nome_pessoa not in erros_map:
                    print(f"[AVISO] '{nome_pessoa}' encontrado na planilha, mas não no USER_MAP. DM não será enviada.")
                    erros_map.append(nome_pessoa)
                continue

            # CRÍTICO: Se o user_id for uma lista, use o primeiro ID para o DM
            if isinstance(user_id, list):
                user_id = user_id[0]

            try:
                user = bot.get_user(user_id) or await bot.fetch_user(user_id)
                if not user:
                    raise Exception("Usuário não encontrado com o ID fornecido.")
                
                # Cria a mensagem e a view
                msg = criar_mensagem_pendencia(
                    pessoa=nome_pessoa,
                    curso=p.get("curso"),
                    tarefa=p.get("tarefa"),
                    dia=p.get("dia")
                )
                # Passa a pendência COMPLETA para a View, que contém o 'row_index'
                view = TaskView(pendencia=p) 
                
                # Envia a DM
                await user.send(msg, view=view)
                print(f"  -> DM enviada para {user.name} ({nome_pessoa}) sobre '{p.get('tarefa')}'")
                sucessos_dm += 1
                
                # Pausa pequena para não sobrecarregar a API do Discord
                await asyncio.sleep(1.5) 

            except discord.errors.Forbidden:
                print(f"[ERRO DM] Falha ao enviar DM para {nome_pessoa} (ID: {user_id}). O usuário pode ter bloqueado o bot ou desabilitado DMs do servidor.")
                falhas_dm += 1
            except Exception as e:
                print(f"[ERRO DM] Erro inesperado ao enviar DM para {nome_pessoa} (ID: {user_id}): {e}")
                falhas_dm += 1
                
        print("Envio de DMs concluído.")

        # Envia um resumo para o canal de log
        if log_channel:
            msg_log = f"📊 **Relatório de Verificação** ({datetime.now().strftime('%d/%m/%Y %H:%M')})\n"
            msg_log += f"- Pendências encontradas na planilha: {pendencias_total}\n"
            msg_log += f"- DMs enviadas com sucesso: {sucessos_dm}\n"
            msg_log += f"- Falhas ao enviar DM: {falhas_dm}\n"
            if erros_map:
                msg_log += f"- Nomes na planilha sem ID no USER_MAP: {', '.join(erros_map)}\n"
            await log_channel.send(msg_log)

    except Exception as e:
        print(f"[ERRO GERAL] Erro ao executar a verificação: {e}")
        if log_channel:
            try:
                await log_channel.send(f"⚠️ **Erro Crítico** ao processar a verificação: `{e}`")
            except:
                pass 

# --- Tarefa Agendada ---
@tasks.loop(time=SCHEDULED_TIMES)
async def run_daily_check():
    """Loop que roda a verificação nos horários agendados."""
    await verificar_pendencias()

# --- Comando Manual (para testes) ---
@bot.slash_command(guild_ids=[GUILD_ID], description="Força uma verificação de pendências e envia DMs.") 
async def verificar(ctx: discord.ApplicationContext):
    """Comando /verificar para rodar manualmente."""
    if not ctx.author.guild_permissions.administrator:
        await ctx.respond("Você não tem permissão para usar este comando.", ephemeral=True)
        return
        
    await ctx.respond("Ok, iniciando uma verificação manual e envio de DMs...", ephemeral=True)
    await verificar_pendencias()

# --- Inicia o Bot ---
if __name__ == "__main__":
    # Garante que todas as bibliotecas necessárias estão instaladas antes de iniciar o bot
    try:
        import dotenv
        import pycord
        import pandas
        import gspread
        import google.oauth2.service_account 
    except ImportError as e:
        print(f"--- ERRO DE BIBLIOTECA ---")
        print(f"Biblioteca faltando: {e.name}")
        print("Rode: pip install -r requirements.txt")
        exit()
        
    if not BOT_TOKEN or not LOG_CHANNEL_ID or not GUILD_ID: 
        print("--- ERRO DE CONFIGURAÇÃO ---")
        print("Verifique se 'BOT_TOKEN', 'LOG_CHANNEL_ID' e 'GUILD_ID' estão no arquivo .env")
    else:
        print("Iniciando o bot...")
        bot.run(BOT_TOKEN)