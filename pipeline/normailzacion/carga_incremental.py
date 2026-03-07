import hashlib
from datetime import datetime

import pandas as pd
from sqlalchemy import bindparam, inspect, text


META_COLUMNS = [
    "hash_clave",
    "hash_contenido",
    "estado_registro",
    "fecha_carga",
    "fecha_actualizacion",
    "fecha_baja",
]

HASH_EXCLUDED_COLUMNS = {
    "hash_clave",
    "hash_contenido",
    "estado_registro",
    "fecha_carga",
    "fecha_actualizacion",
    "fecha_baja",
}


def _is_hash_excluded(column_name):
    return str(column_name).lower() in HASH_EXCLUDED_COLUMNS


def _stringify(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _build_hash(df, columns):
    return df[columns].apply(
        lambda row: hashlib.sha256("|".join(_stringify(v) for v in row.values).encode("utf-8")).hexdigest(),
        axis=1,
    )


def _preparar_dataframe_incremental(df, key_columns, logger):
    df_work = df.copy()
    base_columns = [c for c in df_work.columns if not _is_hash_excluded(c)]

    if not key_columns:
        key_columns = base_columns

    valid_key_columns = [
        c for c in key_columns if c in df_work.columns and not _is_hash_excluded(c)
    ]
    if not valid_key_columns:
        logger.warning(
            "No se encontraron columnas clave de negocio; se usaran todas las columnas como clave."
        )
        valid_key_columns = base_columns
    elif len(valid_key_columns) < len(key_columns):
        logger.warning(
            "Algunas columnas clave no existen en el DataFrame (%s). Se usaran: %s",
            key_columns,
            valid_key_columns,
        )

    now = datetime.now()
    df_work["hash_clave"] = _build_hash(df_work, valid_key_columns)
    df_work["hash_contenido"] = _build_hash(df_work, base_columns)
    df_work["estado_registro"] = "ACTIVO"
    df_work["fecha_carga"] = now
    df_work["fecha_actualizacion"] = now
    df_work["fecha_baja"] = pd.NaT

    return df_work


def _table_has_incremental_columns(engine, schema, table_name):
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema):
        return False, False

    columns = {c["name"] for c in inspector.get_columns(table_name, schema=schema)}
    required = {"hash_clave", "hash_contenido", "estado_registro", "fecha_baja"}
    return True, required.issubset(columns)


def _sql_type_for_series(series):
    """Mapea dtype de pandas a tipo SQL generico para ALTER TABLE."""
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "TEXT"


def _ensure_table_columns(engine, schema, table_name, df_in, logger):
    """Agrega columnas faltantes en tabla destino para soportar evolucion de esquema."""
    inspector = inspect(engine)
    existing_columns = {c["name"] for c in inspector.get_columns(table_name, schema=schema)}
    missing_columns = [c for c in df_in.columns if c not in existing_columns]

    if not missing_columns:
        return

    with engine.begin() as conn:
        for col in missing_columns:
            sql_type = _sql_type_for_series(df_in[col])
            conn.execute(
                text(f'ALTER TABLE {schema}.{table_name} ADD COLUMN "{col}" {sql_type}')
            )
            logger.warning(
                "Tabla %s: columna faltante '%s' agregada automaticamente con tipo %s.",
                table_name,
                col,
                sql_type,
            )


def carga_incremental(engine, schema, table_name, df_nuevo, key_columns, logger):
    """
    Aplica carga incremental con hash_contenido/hash_clave sobre una tabla en PostgreSQL.

    Retorna un dict con conteos de nuevos/modificados/eliminados.
    """
    if df_nuevo is None:
        raise ValueError(f"La tabla '{table_name}' no puede procesarse incrementalmente porque df_nuevo es None")

    df_in = _preparar_dataframe_incremental(df_nuevo, key_columns, logger)

    # Evita estados ambiguos cuando la carga entrante trae la misma clave repetida.
    dup_in = int(df_in["hash_clave"].duplicated().sum())
    if dup_in > 0:
        logger.warning(
            "Tabla %s: se detectaron %d hash_clave duplicados en el lote entrante; se conservara la ultima ocurrencia.",
            table_name,
            dup_in,
        )
        df_in = df_in.drop_duplicates(subset=["hash_clave"], keep="last").reset_index(drop=True)

    exists, has_incremental = _table_has_incremental_columns(engine, schema, table_name)

    # Primer cargue o tabla legacy sin columnas incrementales -> bootstrap.
    if (not exists) or (exists and not has_incremental):
        df_in.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)
        return {
            "nuevos": int(len(df_in)),
            "modificados": 0,
            "eliminados": 0,
            "modo": "bootstrap",
        }

    # Si la tabla ya existe, asegurar que soporte las columnas nuevas del dataset entrante.
    _ensure_table_columns(engine, schema, table_name, df_in, logger)

    query_actual = text(
        f"""
        SELECT hash_clave, hash_contenido
        FROM {schema}.{table_name}
        WHERE estado_registro = 'ACTIVO'
        """
    )
    df_actual = pd.read_sql_query(query_actual, engine)

    # Si hay duplicados activos por hash_clave en historico, colapsarlos para permitir map seguro.
    dup_actual = int(df_actual["hash_clave"].duplicated().sum())
    if dup_actual > 0:
        logger.warning(
            "Tabla %s: se detectaron %d hash_clave duplicados en registros ACTIVO; se usara la ultima ocurrencia para comparar.",
            table_name,
            dup_actual,
        )
        df_actual = df_actual.drop_duplicates(subset=["hash_clave"], keep="last").reset_index(drop=True)

    if df_actual.empty:
        df_in.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        return {
            "nuevos": int(len(df_in)),
            "modificados": 0,
            "eliminados": 0,
            "modo": "append_sin_activos",
        }

    actual_map = df_actual.set_index("hash_clave")["hash_contenido"]

    es_nuevo = ~df_in["hash_clave"].isin(df_actual["hash_clave"])
    df_existente = df_in[~es_nuevo].copy()
    df_existente["hash_contenido_actual"] = df_existente["hash_clave"].map(actual_map)
    es_modificado = df_existente["hash_contenido"] != df_existente["hash_contenido_actual"]

    modified_keys = df_existente.loc[es_modificado, "hash_clave"].dropna().drop_duplicates().tolist()
    deleted_keys = (
        df_actual.loc[~df_actual["hash_clave"].isin(df_in["hash_clave"]), "hash_clave"]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    df_nuevos = df_in[es_nuevo]
    df_modificados = df_in[df_in["hash_clave"].isin(modified_keys)]
    df_insertar = pd.concat([df_nuevos, df_modificados], ignore_index=True)

    with engine.begin() as conn:
        if modified_keys:
            update_modified = text(
                f"""
                UPDATE {schema}.{table_name}
                SET estado_registro = 'REEMPLAZADO',
                    fecha_baja = NOW(),
                    fecha_actualizacion = NOW()
                WHERE estado_registro = 'ACTIVO'
                  AND hash_clave IN :keys
                """
            ).bindparams(bindparam("keys", expanding=True))
            conn.execute(update_modified, {"keys": modified_keys})

        if deleted_keys:
            update_deleted = text(
                f"""
                UPDATE {schema}.{table_name}
                SET estado_registro = 'ELIMINADO',
                    fecha_baja = NOW(),
                    fecha_actualizacion = NOW()
                WHERE estado_registro = 'ACTIVO'
                  AND hash_clave IN :keys
                """
            ).bindparams(bindparam("keys", expanding=True))
            conn.execute(update_deleted, {"keys": deleted_keys})

    if not df_insertar.empty:
        df_insertar.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)

    return {
        "nuevos": int(len(df_nuevos)),
        "modificados": int(len(df_modificados)),
        "eliminados": int(len(deleted_keys)),
        "modo": "incremental",
    }
