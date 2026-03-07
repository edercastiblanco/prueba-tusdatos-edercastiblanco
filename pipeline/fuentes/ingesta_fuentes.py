import os
import zipfile
import logging
import concurrent.futures
from datetime import datetime
from fuentes.extraccion import download_file, extraer_datos_tabla, obtener_debarred_firms, obtener_debarred_firms_rpa

logger = logging.getLogger(__name__)

fuentes = extraer_datos_tabla("configuracion", "fuentes")

def process_source(fila):
    """Procesa una fuente individual: descarga, extrae o scrapea según el formato."""
    inicio = datetime.now()
    url = fila['url']
    nombre_fuente = fila['alias']
    formato = fila['formato'].lower()
    tipo = fila.get('tipo', '').lower()
    estado = 'success'
    error = None

    logger.info("Procesando fuente '%s' con URL: %s, tipo: %s, formato: %s", nombre_fuente, url, tipo, formato)
    try:
        # Priorizar lógica conjunta de tipo y formato
        if tipo == 'rpa' and formato == 'xlsx':
            # RPA siempre genera xlsx
            if obtener_debarred_firms_rpa(url, nombre_fuente, "data/raw"):
                logger.info("Fuente '%s' procesada con RPA exitosamente", nombre_fuente)
            else:
                estado = 'error'
                error = f"Error al procesar RPA para la fuente '{nombre_fuente}'"
                logger.error(error)

        elif tipo == 'url' and formato == 'zip':
            # Descargar el zip
            zip_path = os.path.join("data/raw", f"{nombre_fuente}.zip")
            if download_file(url, zip_path):
                # Descomprimir
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    for file in file_list:
                        if not file.endswith('/'):  # No es un directorio
                            # Extraer y renombrar
                            zip_ref.extract(file, "data/raw")
                            original_name = os.path.basename(file)
                            _, ext = os.path.splitext(original_name)
                            new_name = f"{nombre_fuente}{ext}"
                            extracted_path = os.path.join("data/raw", file)
                            new_path = os.path.join("data/raw", new_name)
                            if os.path.exists(new_path):
                                os.remove(new_path)  # Evitar conflictos
                            os.rename(extracted_path, new_path)
                            logger.info("Archivo extraído y renombrado: %s -> %s", extracted_path, new_path)
                # Eliminar el zip
                os.remove(zip_path)
                logger.info("Fuente '%s' procesada exitosamente", nombre_fuente)
            else:
                estado = 'error'
                error = f"Error al descargar el zip para '{nombre_fuente}'"
                logger.error(error)
        else:
            # Descarga normal
            dest_path = os.path.join("data/raw", f"{nombre_fuente}.{formato}")
            if download_file(url, dest_path):
                logger.info("Fuente '%s' descargada exitosamente a %s", nombre_fuente, dest_path)
            else:
                estado = 'error'
                error = f"Error al descargar la fuente '{nombre_fuente}' desde {url}"
                logger.error(error)
    except Exception as e:
        estado = 'error'
        error = str(e)
        logger.error("Error procesando fuente '%s': %s", nombre_fuente, e)
    finally:
        fin = datetime.now()

    return {
        'fuente': nombre_fuente,
        'tipo': tipo,
        'formato': formato,
        'url': url,
        'inicio': inicio,
        'fin': fin,
        'duracion_segundos': (fin - inicio).total_seconds(),
        'estado': estado,
        'error': error,
    }


def ejecutar_ingesta_inicial_con_metricas():
    """Ejecuta la ingesta inicial en paralelo y retorna metricas por fuente."""
    logger.info("Iniciando ingesta inicial de fuentes en paralelo...")
    metricas = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Crear un diccionario de futuros para rastrear errores por fuente
        futures = {executor.submit(process_source, fila): fila['alias'] for idx, fila in fuentes.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            alias = futures[future]
            try:
                metricas.append(future.result())
            except Exception as e:
                logger.error("Error procesando fuente '%s': %s", alias, e)
                metricas.append({
                    'fuente': alias,
                    'tipo': None,
                    'formato': None,
                    'url': None,
                    'inicio': None,
                    'fin': None,
                    'duracion_segundos': None,
                    'estado': 'error',
                    'error': str(e),
                })

    return metricas


def ejecutar_ingesta_inicial():
    """Compatibilidad: ejecuta la ingesta inicial sin retornar metricas."""
    ejecutar_ingesta_inicial_con_metricas()

if __name__ == "__main__":
    ejecutar_ingesta_inicial()