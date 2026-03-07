import concurrent.futures
import os
import pandas as pd
from normailzacion.carga_incremental import carga_incremental
from normailzacion.normalizacion import (
    csv_paco_penal_to_df,
    json_fcpa_to_df,
    txt_paco_disc_to_df,
    xml_eu_to_df,
    xml_ofac_to_df,
    xml_un_to_df,
    xlsx_banco_mundial_to_df,
)


def persist_dataframe(df, excel_path, table_name, engine, logger, key_columns=None):
    """Guarda un DataFrame en Excel y PostgreSQL (full o incremental)."""
    if df is None:
        raise ValueError(f"La fuente para la tabla '{table_name}' devolvio None")

    df.to_excel(excel_path, index=False)

    carga_incremental_habilitada = os.getenv("CARGA_INCREMENTAL", "true").lower() == "true"
    if carga_incremental_habilitada:
        resultado = carga_incremental(
            engine=engine,
            schema="bronze",
            table_name=table_name,
            df_nuevo=df,
            key_columns=key_columns,
            logger=logger,
        )
        logger.info(
            "Carga incremental %s -> nuevos=%d, modificados=%d, eliminados=%d (modo=%s)",
            table_name,
            resultado["nuevos"],
            resultado["modificados"],
            resultado["eliminados"],
            resultado["modo"],
        )
    else:
        df.to_sql(table_name, engine, schema="bronze", if_exists="replace", index=False)
        logger.info("Carga full replace aplicada para tabla %s", table_name)


def run_normalizaciones(engine, logger, run_id):
    """Ejecuta las normalizaciones definidas en el plan de tareas, en paralelo."""
    tareas_normalizacion = [
        ("UN", xml_un_to_df, "data/normalizado/UN_procesado.xlsx", "un", ["numero_documento", "referencia"]),
        ("EU", xml_eu_to_df, "data/normalizado/EU_procesado.xlsx", "eu", ["numero_documento", "logical_id"]),
        ("FCPA", json_fcpa_to_df, "data/normalizado/FCPA_procesado.xlsx", "fcpa", ["numero_documento"]),
        (
            "PACO_DISC",
            txt_paco_disc_to_df,
            "data/normalizado/PACO_DISC_procesado.xlsx",
            "paco_disc",
            ["numero_documento", "NUM_PROVIDENCIA"],
        ),
        (
            "PACO_PENAL",
            csv_paco_penal_to_df,
            "data/normalizado/PACO_PENAL_procesado.xlsx",
            "paco_penal",
            ["id"],
        ),
        (
            "OFAC",
            xml_ofac_to_df,
            "data/normalizado/OFAC_unificado.xlsx",
            "ofac_unificado",
            ["entity_id", "numero_documento"],
        ),
        (
            "BANCO_MUNDIAL",
            xlsx_banco_mundial_to_df,
            "data/normalizado/BANCO_MUNDIAL_procesado.xlsx",
            "banco_mundial",
            ["nombres", "nacionalidad", "tipo_sancion"],
        ),
    ]

    def _procesar_tarea(nombre, transform_fn, excel_path, table_name, key_columns):
        inicio = pd.Timestamp.now()
        logger.info("Iniciando proceso de normalizacion de datos %s...", nombre)
        estado = "success"
        error = None
        filas = None
        try:
            df = transform_fn()
            filas = int(len(df)) if df is not None else None
            persist_dataframe(df, excel_path, table_name, engine, logger, key_columns=key_columns)
        except Exception as exc:
            estado = "error"
            error = str(exc)
            raise
        finally:
            fin = pd.Timestamp.now()
            duracion = (fin - inicio).total_seconds()

        logger.info(
            "Proceso de normalizacion de datos %s completado en %.2f segundos.",
            nombre,
            duracion,
        )

        return {
            "run_id": run_id,
            "etapa": "normalizacion",
            "fuente": nombre,
            "tabla_destino": table_name,
            "inicio": inicio,
            "fin": fin,
            "duracion_segundos": duracion,
            "filas": filas,
            "estado": estado,
            "error": error,
        }

    max_workers = int(os.getenv("NORMALIZACION_MAX_WORKERS", str(min(4, len(tareas_normalizacion)))))
    metricas = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_procesar_tarea, nombre, transform_fn, excel_path, table_name, key_columns)
            for nombre, transform_fn, excel_path, table_name, key_columns in tareas_normalizacion
        ]
        for future in concurrent.futures.as_completed(futures):
            metricas.append(future.result())

    return metricas
