#!/bin/bash

echo "🚀 Spouštím celou Haystack infrastrukturu..."

# Nadefinujeme si cestu k tvému projektu
PROJECT_DIR="~/school/2LS/SKJ/object_storage"

# 1. Spuštění S3 Gateway (main.py) v novém okně
cmd.exe /c start wsl.exe -e bash -c "cd $PROJECT_DIR && source venv/bin/activate && python3 main.py; exec bash"

echo "⏳ Čekám 2 vteřiny na nastartování Message Brokera..."
sleep 2

# 2. Spuštění Haystack Node (haystack.py) v novém okně
cmd.exe /c start wsl.exe -e bash -c "cd $PROJECT_DIR && source venv/bin/activate && python3 haystack.py; exec bash"

# 3. Spuštění Workera (worker.py) v novém okně
cmd.exe /c start wsl.exe -e bash -c "cd $PROJECT_DIR && source venv/bin/activate && python3 worker.py; exec bash"

echo "🌐 Otevírám Front-end v prohlížeči..."
explorer.exe index.html

echo "✅ Všechny služby běží a UI je otevřené!"