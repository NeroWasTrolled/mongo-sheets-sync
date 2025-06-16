@echo off
echo ================================
echo Iniciando Sincronizador UniCV...
echo ================================

REM Ativa o ambiente Python (se precisar) ou usa o Python do sistema:
REM Se você usar virtualenv, ative aqui:
REM call venv\Scripts\activate

REM Executa o script Python
python Database.py

pause
