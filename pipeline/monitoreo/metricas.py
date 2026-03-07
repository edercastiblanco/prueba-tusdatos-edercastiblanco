import json
import os

import pandas as pd
from sqlalchemy import text


def persistir_metricas(engine, metricas, logger):
    """Crea esquema/tabla de monitoreo si no existe y persiste metricas."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS monitoreo"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS monitoreo.metricas (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    etapa TEXT NOT NULL,
                    fuente TEXT,
                    tabla_destino TEXT,
                    inicio TIMESTAMP,
                    fin TIMESTAMP,
                    duracion_segundos DOUBLE PRECISION,
                    filas BIGINT,
                    estado TEXT,
                    error TEXT
                )
                """
            )
        )

    df_metricas = pd.DataFrame(metricas)
    df_metricas.to_sql("metricas", engine, schema="monitoreo", if_exists="append", index=False)
    logger.info("Metricas guardadas en monitoreo.metricas (%d filas).", len(df_metricas))

    # Exportar tambien a JSON por ejecucion en el filesystem local.
    run_id = str(df_metricas["run_id"].iloc[0]) if not df_metricas.empty else "sin_run_id"
    output_dir = os.path.join("pipeline", "monitoreo", "metricas")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{run_id}.json")

    df_export = df_metricas.copy()
    for col in ["inicio", "fin"]:
        if col in df_export.columns:
            df_export[col] = pd.to_datetime(df_export[col], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
            df_export[col] = df_export[col].where(df_export[col].notna(), None)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(df_export.to_dict(orient="records"), f, indent=2)

    logger.info("Metricas exportadas a JSON en %s", output_path)
