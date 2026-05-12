import asyncio
from pathlib import Path
import os
import sqlite3

DB_PATH = "./sql_app.db"
VOLUME_DIR = Path("volumes")

async def run_compaction():
    print("🧹 Zahajuji defragmentaci po segmentech...")
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
        # Očista databáze pro jistotu
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

    # 3. Procházení znečištěných disků
    for old_vol_id in volumes_to_compact:
        old_vol_path = VOLUME_DIR / f"volume_{old_vol_id}.dat"
        segments_to_keep = volumes_map.get(old_vol_id, [])
        
        if not old_vol_path.exists(): continue

        # Pokud jsou na disku POUZE smazaná data, rovnou ho celý vymažeme
        if not segments_to_keep:
            print(f"\n🗑️ Disk volume_{old_vol_id}.dat má jen smazaná data. Mažu ho celý...")
            os.remove(old_vol_path)
            cursor.execute("DELETE FROM file_segments WHERE volume_id = ?", (old_vol_id,))
            conn.commit()
            continue

        # Pokud disk obsahuje mix živých a smazaných dat, přesuneme živá jinam
        new_vol_id = old_vol_id + 1000 
        new_vol_path = VOLUME_DIR / f"volume_{new_vol_id}.dat"
        
        print(f"\n📦 Přesouvám svazek {old_vol_id} -> do nového {new_vol_id}")
        current_new_offset = 0
        
        with open(old_vol_path, "rb") as f_old, open(new_vol_path, "wb") as f_new:
            for seg_data in segments_to_keep:
                f_old.seek(seg_data["offset"])
                data = f_old.read(seg_data["size"])
                f_new.write(data)
                
                cursor.execute("UPDATE file_segments SET volume_id = ?, offset = ? WHERE id = ?", (new_vol_id, current_new_offset, seg_data["id"]))
                current_new_offset += seg_data["size"]
                
        os.remove(old_vol_path)
        cursor.execute("DELETE FROM file_segments WHERE volume_id = ? AND file_id IN (SELECT id FROM files WHERE is_deleted = 1)", (old_vol_id,))
        conn.commit()
        print(f"✅ Svazek volume_{old_vol_id}.dat přepsán bez děr.")

    # 4. KLÍČOVÁ OPRAVA: Smažeme z DB "duchy" (záznamy o souborech, které po defragmentaci už nemají žádné fyzické bloky)
    cursor.execute("DELETE FROM files WHERE is_deleted = 1 AND id NOT IN (SELECT file_id FROM file_segments)")
    conn.commit()

    conn.close()

if __name__ == "__main__":
    asyncio.run(run_compaction())