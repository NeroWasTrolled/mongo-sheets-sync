from datetime import datetime
from pymongo import MongoClient
import os
import json
import csv
from bson import ObjectId

# === CONFIGURAÇÕES ===

MONGO_URI = "mongodb+srv://manager-64a56365aaedd19fa72d2787:P8eUi16vSs0eeaRm@unicvcluster.mxp18.mongodb.net/ws-64a56365aaedd19fa72d2787?retryWrites=true&w=majority"
DB_NAME = "ws-64a56365aaedd19fa72d2787"
COLLECTION_NAME = "workspace-64a56365aaedd19fa72d2787-ead"

# === SERIALIZAÇÃO ===

def convert_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

def salvar_backup_json(dados, nome_base="dados"):
    from bson.json_util import dumps
    os.makedirs("backups", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    caminho = f"backups/backup_{nome_base}_{timestamp}.json"
    dados_convertidos = dumps(dados, indent=2)
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(dados_convertidos)
    print(f"🗃️ Backup JSON salvo em {caminho}")

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
    print(f"📁 Backup CSV salvo em {caminho}")

# === EXECUTA BACKUP ===

if __name__ == "__main__":
    print("🔄 Iniciando backup manual...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    documentos = list(collection.find({}))
    if not documentos:
        print("⚠️ Nenhum dado encontrado.")
    else:
        salvar_backup_json(documentos, nome_base=COLLECTION_NAME)
        salvar_backup_csv(documentos, nome_base=COLLECTION_NAME)
        print(f"✅ Backup manual concluído com {len(documentos)} registros.")
