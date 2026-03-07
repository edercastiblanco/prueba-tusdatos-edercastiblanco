import logging
import pandas as pd
from db.conexion import build_engine
from fuentes.ingesta_fuentes import ejecutar_ingesta_inicial_con_metricas
from logging_config import setup_logging
from monitoreo.metricas import persistir_metricas
from normailzacion.ejecucion_normalizacion import run_normalizaciones


def run_ingesta(logger, run_id):
    """Ejecuta la etapa de ingesta y registra su duracion."""
    inicio_ingesta = pd.Timestamp.now()
    logger.info("Iniciando proceso de ingesta de datos...")
    status = "success"
    error_message = None
    metricas_fuente = []
    try:
        metricas_fuente = ejecutar_ingesta_inicial_con_metricas()
        if any(m.get("estado") == "error" for m in metricas_fuente):
            status = "error"
            error_message = "Una o mas fuentes fallaron durante la ingesta"
    except Exception as exc:
        status = "error"
        error_message = str(exc)
        raise
    finally:
        fin_ingesta = pd.Timestamp.now()
        duracion = (fin_ingesta - inicio_ingesta).total_seconds()
        logger.info("Duracion total de la ingesta: %.2f segundos", duracion)

    metricas = [
        {
            "run_id": run_id,
            "etapa": "ingesta",
            "fuente": "TODAS",
            "tabla_destino": None,
            "inicio": inicio_ingesta,
            "fin": fin_ingesta,
            "duracion_segundos": duracion,
            "filas": None,
            "estado": status,
            "error": error_message,
        }
    ]

    for metrica in metricas_fuente:
        metricas.append(
            {
                "run_id": run_id,
                "etapa": "ingesta_fuente",
                "fuente": metrica.get("fuente"),
                "tabla_destino": None,
                "inicio": metrica.get("inicio"),
                "fin": metrica.get("fin"),
                "duracion_segundos": metrica.get("duracion_segundos"),
                "filas": None,
                "estado": metrica.get("estado"),
                "error": metrica.get("error"),
            }
        )

    return metricas


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        engine = build_engine()
    except Exception as e:
        logger.exception("Error al conectar a la base de datos: %s", e)
        raise

    logger.info("Sistema iniciado desde la raiz. Cargando Pipeline...")

    run_id = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    metricas = []
    metricas.extend(run_ingesta(logger, run_id))
    metricas.extend(run_normalizaciones(engine, logger, run_id))
    persistir_metricas(engine, metricas, logger)

    total_segundos = sum(m.get("duracion_segundos", 0) or 0 for m in metricas)
    fuentes_ok = sum(1 for m in metricas if m.get("estado") == "success")
    logger.info(
        "Resumen pipeline run_id=%s | etapas=%d | exitosas=%d | duracion_total=%.2fs",
        run_id,
        len(metricas),
        fuentes_ok,
        total_segundos,
    )

    logger.info("Pipeline finalizado exitosamente.")


if __name__ == "__main__":
    main()