# 📊 Hablla Reports Sync

**Sincronizador automático para exportar registros do MongoDB do Hablla para o Google Sheets**, separado por mês real de criação. Organiza dados históricos, faz backups automáticos e mantém relatórios atualizados em tempo real!

---

## ✨ Funcionalidades

✅ Conexão segura com MongoDB Atlas  
✅ Atualiza Google Sheets separando dados por mês (`created_at`)  
✅ Cria abas mensais automaticamente  
✅ Limpa e sincroniza registros de forma confiável (modo espelho)  
✅ Backups automáticos em JSON e CSV  
✅ Log detalhado de cada sincronização  
✅ Roda em loop infinito com intervalo configurável

---

## 🚀 Como usar

### 1️⃣ Clone o repositório

```git clone https://github.com/NeroWasTrolled/mongo-sheets-sync.git```

```cd mongo-sheets-sync```

### 2️⃣ Instale as dependências

```pip install pymongo gspread oauth2client python-dateutil```

### 3️⃣ Configure
- Coloque seu credencial-google.json na pasta raiz.
- Ajuste as variáveis no Database.py:
- - MONGO_URI
- - DB_NAME
- - COLLECTION_NAME
- - GOOGLE_SHEETS_NOME

### 4️⃣ Execute
```python Database.py```

ou use o .bat:

```start_sync.bat```

### 🗂️ Estrutura de backup
Todos os backups ficam na pasta /backups:

```backup_<NOME_COLECAO>_<data>.json```

```backup_<NOME_COLECAO>_<data>.csv```

### 📝 Logs
As ações são salvas em log_sync.txt:
- Início de sincronização
- Quantidade total de registros enviados
- Quantos são realmente novos
- Erros, se houver

# ⚙️ Automação recomendada
Crie uma Tarefa Agendada no Windows ou um serviço no Linux para rodar o script automaticamente no boot.
