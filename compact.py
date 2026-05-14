import asyncio
from pathlib import Path
import os
import sqlite3

DB_PATH = "./sql_app.db"
VOLUME_DIR = Path("volumes")
MAX_VOLUME_SIZE = 1 * 1024 * 1024  # 1 MB limit (aby se svazky zase rozdělily, pokud přesáhnou limit)

async def run_compaction():
    print("🧹 Zahajuji MERGING defragmentaci (slučování poloprázdných svazků)...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Které disky obsahují smazané části?
    cursor.execute("""
        SELECT DISTINCT s.volume_id 
        FROM file_segments s 
        JOIN files f ON s.file_id = f.id 
        WHERE f.is_deleted = 1
    """)
    volumes_to_compact = [row[0] for row in cursor.fetchall()]
    
    if not volumes_to_compact:
        print("ℹ️ Žádný svazek nepotřebuje úklid.")
        cursor.execute("DELETE FROM files WHERE is_deleted = 1 AND id NOT IN (SELECT file_id FROM file_segments)")
        conn.commit()
        return

    # 2. Vytažení všech zdravých segmentů, které chceme na discích zachovat
    cursor.execute("""
        SELECT s.id, s.volume_id, s.offset, s.size 
        FROM file_segments s
        JOIN files f ON s.file_id = f.id
        WHERE f.is_deleted = 0 AND f.status = 'ready'
        ORDER BY s.volume_id, s.offset
    """)
    active_segments = cursor.fetchall()
    
    volumes_map = {}
    for s_id, vol_id, offset, size in active_segments:
        if vol_id not in volumes_map: volumes_map[vol_id] = []
        volumes_map[vol_id].append({"id": s_id, "offset": offset, "size": size})

    # --- NOVÁ LOGIKA PRO SLUČOVÁNÍ (MERGING) ---
    
    # Zjistíme nejvyšší aktuální ID svazku, ať můžeme vytvořit úplně nové pokračování
    cursor.execute("SELECT MAX(volume_id) FROM file_segments")
    max_vol = cursor.fetchone()[0] or 0
    new_merged_vol_id = max_vol + 1
    new_merged_vol_path = VOLUME_DIR / f"volume_{new_merged_vol_id}.dat"
    current_new_offset = 0
    
    # Otevřeme první nový sloučený svazek pro zápis
    f_new = open(new_merged_vol_path, "wb")
    print(f"\n📦 Vytvářím nový sloučený svazek: volume_{new_merged_vol_id}.dat")

    for old_vol_id in volumes_to_compact:
        old_vol_path = VOLUME_DIR / f"volume_{old_vol_id}.dat"
        segments_to_keep = volumes_map.get(old_vol_id, [])
        
        if not old_vol_path.exists(): continue

        # Pokud má jen smazaná data, rovnou ho celý vymažeme a jdeme dál
        if not segments_to_keep:
            print(f"🗑️ Disk volume_{old_vol_id}.dat má jen smazaná data. Mažu ho celý...")
            os.remove(old_vol_path)
            cursor.execute("DELETE FROM file_segments WHERE volume_id = ?", (old_vol_id,))
            conn.commit()
            continue

        print(f"🔄 Nasávám přeživší data ze svazku {old_vol_id}...")
        
        with open(old_vol_path, "rb") as f_old:
            for seg_data in segments_to_keep:
                
                # Kontrola: Pokud bychom přidáním další fotky přesáhli 1 MB, svazek uzavřeme a vytvoříme další
                if current_new_offset + seg_data["size"] > MAX_VOLUME_SIZE and current_new_offset > 0:
                    f_new.close()
                    new_merged_vol_id += 1
                    new_merged_vol_path = VOLUME_DIR / f"volume_{new_merged_vol_id}.dat"
                    f_new = open(new_merged_vol_path, "wb")
                    current_new_offset = 0
                    print(f"📦 Svazek plný, rotuji. Vytvářím další svazek: volume_{new_merged_vol_id}.dat")

                # Přečteme ze starého a zapisujeme do sloučeného
                f_old.seek(seg_data["offset"])
                data = f_old.read(seg_data["size"])
                f_new.write(data)
                
                # Aktualizace databáze
                cursor.execute("UPDATE file_segments SET volume_id = ?, offset = ? WHERE id = ?", (new_merged_vol_id, current_new_offset, seg_data["id"]))
                current_new_offset += seg_data["size"]
                
        # Po přesunu dat starý soubor smažeme
        os.remove(old_vol_path)
        cursor.execute("DELETE FROM file_segments WHERE volume_id = ? AND file_id IN (SELECT id FROM files WHERE is_deleted = 1)", (old_vol_id,))
        conn.commit()

    f_new.close()
    print(f"\n✅ Sloučení úspěšně dokončeno!")

    # Odstraníme z DB záznamy o souborech, které už nemají žádná fyzická data
    cursor.execute("DELETE FROM files WHERE is_deleted = 1 AND id NOT IN (SELECT file_id FROM file_segments)")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    asyncio.run(run_compaction())