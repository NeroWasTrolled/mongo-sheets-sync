import time
from datetime import datetime
from pymongo import MongoClient
import gspread
import os
import json
import csv
from bson import ObjectId
from dateutil import parser
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials

# === CONFIGURAÇÕES ===

MONGO_URI = "mongodb+srv://manager-64a56365aaedd19fa72d2787:P8eUi16vSs0eeaRm@unicvcluster.mxp18.mongodb.net/ws-64a56365aaedd19fa72d2787?retryWrites=true&w=majority"
DB_NAME = "ws-64a56365aaedd19fa72d2787"
COLLECTION_NAME = "workspace-64a56365aaedd19fa72d2787-ead"

GOOGLE_SHEETS_JSON = "credencial-google.json"
GOOGLE_SHEETS_NOME = "Relatorio UniCV"

INTERVALO_MINUTOS = 5
CAMPO_CHAVE = "_id"

# === FUNÇÃO DE LOG ===

def log_mensagem(texto):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("log_sync.txt", "a", encoding="utf-8") as log:
        log.write(f"[{timestamp}] {texto}\n")

# === SERIALIZAÇÃO DE OBJECTID ===

def convert_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

# === BACKUP EM JSON E CSV ===

def salvar_backup_json(dados, nome_base="dados"):
    from bson.json_util import dumps
    os.makedirs("backups", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    caminho = f"backups/backup_{nome_base}_{timestamp}.json"
    dados_convertidos = dumps(dados, indent=2)
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(dados_convertidos)
    log_mensagem(f"🗃️ Backup JSON salvo em {caminho}")
    return caminho

def salvar_backup_csv(dados, nome_base="dados"):
    os.makedirs("backups", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    caminho = f"backups/backup_{nome_base}_{timestamp}.csv"
    if not dados:
        return
    todos_os_campos = set()
    for doc in dados:
        todos_os_campos.update(doc.keys())
    todos_os_campos = sorted(list(todos_os_campos))
    with open(caminho, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=todos_os_campos)
        writer.writeheader()
        for row in dados:
            row_formatado = {chave: str(row.get(chave, "")) for chave in todos_os_campos}
            writer.writerow(row_formatado)
    log_mensagem(f"📁 Backup CSV salvo em {caminho}")
    return caminho

# === LIMPAR BACKUPS ANTIGOS ===

def limpar_backups_antigos(max_arquivos=10):
    arquivos = sorted(os.listdir("backups"), key=lambda f: os.path.getmtime(os.path.join("backups", f)))
    excessos = arquivos[:-max_arquivos]
    for arquivo in excessos:
        caminho = os.path.join("backups", arquivo)
        try:
            os.remove(caminho)
            log_mensagem(f"🧹 Backup antigo removido: {caminho}")
        except Exception as e:
            log_mensagem(f"⚠️ Erro ao remover backup antigo: {caminho} - {e}")

# === FUNÇÃO PRINCIPAL ===

def atualizar_relatorio():
    print("🔄 Iniciando atualização com separação por created_at...")
    log_mensagem("🔄 Iniciando atualização com separação por created_at...")

    try:
        # 1. Conectar ao MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        documentos = list(collection.find({}))

        if not documentos:
            msg = "⚠️ Nenhum dado encontrado na coleção."
            print(msg)
            log_mensagem(msg)
            return

        # 2. Backup e limpeza de backups
        salvar_backup_json(documentos, nome_base=COLLECTION_NAME)
        salvar_backup_csv(documentos, nome_base=COLLECTION_NAME)
        limpar_backups_antigos(max_arquivos=10)

        # 3. Conectar ao Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        credenciais = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_JSON, scope)
        gs_client = gspread.authorize(credenciais)
        planilha = gs_client.open(GOOGLE_SHEETS_NOME)

        # 4. Agrupar docs por ABA com base no created_at
        docs_por_aba = defaultdict(list)

        for doc in documentos:
            created_at = doc.get("created_at")
            dt = None
            if isinstance(created_at, dict) and "$date" in created_at:
                dt = parser.parse(created_at["$date"])
            elif isinstance(created_at, str):
                dt = parser.parse(created_at)
            elif isinstance(created_at, datetime):
                dt = created_at
            else:
                dt = datetime.now()

            nome_aba = f"Página1_{dt.strftime('%Y_%m')}"
            docs_por_aba[nome_aba].append(doc)

        # 5. Para cada ABA, processar
        for nome_aba, docs in docs_por_aba.items():
            # Tentar abrir ou criar a aba
            try:
                sheet = planilha.worksheet(nome_aba)
            except gspread.WorksheetNotFound:
                sheet = planilha.add_worksheet(title=nome_aba, rows="1000", cols="50")
                log_mensagem(f"🗂️ Nova aba criada: {nome_aba}")

            # Pegar IDs antigos da aba antes de limpar
            valores_antigos = sheet.get_all_records()
            ids_antigos = {str(row.get(CAMPO_CHAVE, "")).strip() for row in valores_antigos}

            # Gerar headers e dados
            todos_os_campos = set()
            for d in docs:
                todos_os_campos.update(d.keys())
            headers = sorted(list(todos_os_campos))
            dados = [headers] + [[str(d.get(h, "")) for h in headers] for d in docs]

            # Limpar e atualizar a aba do mês real
            sheet.clear()
            sheet.update(values=dados, range_name="A1")

            # Contar novos
            ids_novos = {str(d.get(CAMPO_CHAVE, "")).strip() for d in docs}
            novos_criados = len(ids_novos - ids_antigos)

            msg = f"✅ Aba [{nome_aba}] atualizada: {len(docs)} registros totais | {novos_criados} novos."
            print(msg)
            log_mensagem(msg)

    except Exception as e:
        erro = f"❌ Erro durante a atualização: {e}"
        print(erro)
        log_mensagem(erro)

# === LOOP DE EXECUÇÃO ===

if __name__ == "__main__":
    while True:
        atualizar_relatorio()
        print(f"⏳ Aguardando {INTERVALO_MINUTOS} minutos...\n")
        time.sleep(60 * INTERVALO_MINUTOS)
