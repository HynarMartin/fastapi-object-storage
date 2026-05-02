import asyncio
from pathlib import Path
import os
import sqlite3

DB_PATH = "./sql_app.db"
VOLUME_DIR = Path("volumes")

async def run_compaction():
    print("🧹 Zahajuji proces kompakce (defragmentace) Haystack svazků...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Zjistíme, které svazky obsahují smazané soubory (ty potřebují úklid)
    cursor.execute("SELECT DISTINCT volume_id FROM files WHERE is_deleted = 1 AND volume_id IS NOT NULL")
    volumes_to_compact = [row[0] for row in cursor.fetchall()]
    
    if not volumes_to_compact:
        print("ℹ️ Žádný svazek nepotřebuje defragmentaci (nejsou v nich smazané soubory).")
        conn.close()
        return

    # 2. Vytáhneme si mapu VŠECH živých souborů
    cursor.execute("""
        SELECT id, volume_id, offset, size 
        FROM files 
        WHERE is_deleted = 0 AND status = 'ready' AND volume_id IS NOT NULL
        ORDER BY volume_id, offset
    """)
    active_files = cursor.fetchall()
    
    volumes_map = {}
    for f_id, vol_id, offset, size in active_files:
        if vol_id not in volumes_map:
            volumes_map[vol_id] = []
        volumes_map[vol_id].append({"id": f_id, "offset": offset, "size": size})

    # 3. Projdeme svazky, které je třeba uklidit
    for old_vol_id in volumes_to_compact:
        old_vol_path = VOLUME_DIR / f"volume_{old_vol_id}.dat"
        files_to_keep = volumes_map.get(old_vol_id, [])
        
        # Ošetření: Svazek na disku už fyzicky není
        if not old_vol_path.exists():
            cursor.execute("DELETE FROM files WHERE volume_id = ? AND is_deleted = 1", (old_vol_id,))
            conn.commit()
            continue

        # EDGE CASE: Ve svazku nejsou ŽÁDNÉ živé soubory (všechno bylo smazáno)
        if not files_to_keep:
            print(f"\n🗑️ Svazek {old_vol_id} obsahuje POUZE smazané soubory. Mažu ho celý...")
            old_size = old_vol_path.stat().st_size
            os.remove(old_vol_path)
            cursor.execute("DELETE FROM files WHERE volume_id = ? AND is_deleted = 1", (old_vol_id,))
            conn.commit()
            print(f"✅ Svazek {old_vol_id} a jeho databázové záznamy byly kompletně smazány.")
            print(f"📉 Ušetřeno místa: {old_size} bajtů.")
            continue

        # KLASICKÁ KOMPAKCE: Ve svazku je mix živých a smazaných fotek
        new_vol_id = old_vol_id + 1000 
        new_vol_path = VOLUME_DIR / f"volume_{new_vol_id}.dat"
        
        print(f"\n📦 Kompaktuji svazek {old_vol_id} -> do nového svazku {new_vol_id}")
        print(f"📊 Počet živých souborů k přesunu: {len(files_to_keep)}")
        
        current_new_offset = 0
        with open(old_vol_path, "rb") as f_old, open(new_vol_path, "wb") as f_new:
            for file_data in files_to_keep:
                # Přečteme stará data a zapíšeme do nového
                f_old.seek(file_data["offset"])
                data = f_old.read(file_data["size"])
                f_new.write(data)
                
                # Aktualizace v DB
                cursor.execute(
                    "UPDATE files SET volume_id = ?, offset = ? WHERE id = ?", 
                    (new_vol_id, current_new_offset, file_data["id"])
                )
                current_new_offset += file_data["size"]
                
        # Smazání starého souboru a smazaných "duchů" v DB
        old_size = old_vol_path.stat().st_size
        new_size = new_vol_path.stat().st_size
        os.remove(old_vol_path)
        cursor.execute("DELETE FROM files WHERE volume_id = ? AND is_deleted = 1", (old_vol_id,))
        conn.commit()
        
        print(f"✅ Svazek {old_vol_id} byl smazán.")
        print(f"📉 Ušetřeno místa: {old_size - new_size} bajtů.")

    conn.close()
    print("\n🎉 Kompakce úspěšně dokončena!")

if __name__ == "__main__":
    asyncio.run(run_compaction())