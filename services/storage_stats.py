from datetime import datetime, timedelta
import os
import shutil

from db import get_db_connection
from services.disk_reaming import REPOSITORY_PATH, get_free_space_bytes, DAYS_TO_AVERAGE


def get_storage_stats():
    """Calcula métricas de armazenamento do repositório.

    Retorna:
        dict: {
            total_mb: float,
            total_gb: float,
            avg_mb_per_day_30: float,
            capacity_mb: float | None,
            free_mb: float | None,
            days_remaining: int | None,
            primary_dir: str | None
        }
    """

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Tamanho total apenas dos diretórios padrão (fs_status = 0)
        cur.execute(
            """
            SELECT COALESCE(SUM(f.file_size), 0)
            FROM files f
            JOIN filesystem fs ON f.filesystem_fk = fs.pk
            WHERE fs.fs_status = 0
            """
        )
        total_bytes = cur.fetchone()[0] or 0

        # Média diária de arquivamento dos últimos DAYS_TO_AVERAGE dias (igual ao disk_reaming)
        cur.execute(
            f"""
            WITH daily_usage AS (
                SELECT
                    CAST(s.created_time AS DATE) AS archive_date,
                    SUM(f.file_size) AS total_bytes_per_day
                FROM study s
                JOIN series se ON s.pk = se.study_fk
                JOIN instance i ON se.pk = i.series_fk
                JOIN files f ON i.pk = f.instance_fk
                JOIN filesystem fs ON f.filesystem_fk = fs.pk
                WHERE s.created_time >= NOW() - INTERVAL '{DAYS_TO_AVERAGE} days'
                  AND fs.fs_status = 0
                GROUP BY archive_date
            )
            SELECT COALESCE(AVG(total_bytes_per_day), 0) FROM daily_usage
            """
        )
        avg_bytes_per_day = float(cur.fetchone()[0] or 0)

        # Diretório principal (para obter capacidade do disco)
        cur.execute("SELECT dirpath FROM filesystem WHERE fs_status = 0 ORDER BY fs_status ASC LIMIT 1")
        row = cur.fetchone()
        primary_dir = row[0] if row else None
    finally:
        cur.close()
        conn.close()

    total_mb = round(total_bytes / 1024 / 1024, 2)
    total_gb = round(total_bytes / 1024 / 1024 / 1024, 2)
    avg_mb_per_day_30 = round((avg_bytes_per_day / 1024 / 1024), 2)

    capacity_mb = None
    free_mb = None
    days_remaining = None

    # Tenta obter capacidade e espaço livre do disco do diretório principal
    try:
        # Espaço livre seguindo disk_reaming
        free_bytes = get_free_space_bytes(REPOSITORY_PATH)
        if free_bytes is not None:
            free_mb = round(free_bytes / 1024 / 1024, 2)
            if os.path.exists(REPOSITORY_PATH):
                usage = shutil.disk_usage(REPOSITORY_PATH)
                capacity_mb = round(usage.total / 1024 / 1024, 2)
            if avg_mb_per_day_30 > 0:
                days_remaining = int((free_bytes / 1024 / 1024) / avg_mb_per_day_30)
    except Exception:
        # Caso não seja possível obter dados do disco (ex.: caminho de rede), mantém None
        pass

    return {
        'total_mb': total_mb,
        'total_gb': total_gb,
        'avg_mb_per_day_30': avg_mb_per_day_30,
        'capacity_mb': capacity_mb,
        'free_mb': free_mb,
        'days_remaining': days_remaining,
        'primary_dir': primary_dir,
    }