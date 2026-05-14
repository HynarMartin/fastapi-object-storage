#!/bin/bash

echo "🧹 Zahajuji kompletní úklid systému..."

# Smazání databází (parametr -f zajistí, že to nevyhodí chybu, pokud už jsou smazané)
rm -f sql_app.db
rm -f test_db.db

# Smazání všech fyzických Haystack svazků
rm -f volumes/*.dat

# Spuštění tvého inicializačního skriptu
python3 init_db.py

echo "✨ Hotovo! Systém je čistý a připravený na testování."