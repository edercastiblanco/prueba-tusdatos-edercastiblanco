import os
import requests
import logging
from dotenv import load_dotenv
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, text
import pandas as pd
from playwright.sync_api import Playwright, TimeoutError,sync_playwright

logger = logging.getLogger(__name__)

# Carga las variables del archivo .env al entorno de sistema
load_dotenv()

# Extraer las variables usando os.getenv
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

print(f"Configuración de DB: host={db_host}, port={db_port}, user={db_user}, db={db_name}")


# Cadena de agente de usuario razonable para evitar bloqueos simples por bots
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"

def get_session():
    """
    Configura y retorna una sesión de requests optimizada con reintentos y headers de navegador.

    Esta función inicializa una sesión que incluye una estrategia de reintentos exponenciales
    para errores específicos del servidor (500, 502, 503, 504) y monta adaptadores HTTP/HTTPS.
    También inyecta cabeceras (User-Agent, Accept) para mimetizar el comportamiento de un
    navegador real y reducir bloqueos 403.

    Returns:
        requests.Session: Un objeto de sesión configurado con persistencia de conexión
            y lógica de reintento automático.

    Notes:
        La estrategia de reintento usa un `backoff_factor=1`, lo que significa que las
        esperas entre intentos serán de 1s, 2s, 4s respectivamente.
    """
    session = requests.Session()
    # Definimos 3 reintentos si hay errores 500, 502, 503 o 504
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # Cabeceras por defecto tipo navegador para reducir posibilidad de 403
    session.headers.update({
        "User-Agent": BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return session

def download_file(url, dest_path):
    """Descarga un archivo y lo guarda en la ruta local. Retorna True/False y registra eventos."""
    session = get_session()
    try:
        # Asegurar que la carpeta destino exista
        dest_dir = os.path.dirname(dest_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

        logger.info("Iniciando descarga: %s -> %s", url, dest_path)
        response = session.get(url, timeout=30, stream=True)
        # Manejo especial para 403: intentar una vez más con cabeceras adicionales (Referer explícito)
        if response.status_code == 403:
            logger.warning("Recibido 403 para %s. Reintentando con cabeceras tipo navegador...", url)
            response = session.get(url, timeout=30, stream=True, headers={
                "User-Agent": BROWSER_UA,
                "Referer": url,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            })

        # Si sigue siendo error, registrar y devolver False
        if response.status_code >= 400:
            logger.error("Error HTTP %s al descargar %s", response.status_code, url)
            return False

        # Guardar contenido
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info("Descarga exitosa: %s", dest_path)
        return True
    except requests.exceptions.RequestException as e:
        logger.exception("Error de requests descargando %s -> %s: %s", url, dest_path, e)
        return False
    except Exception as e:
        logger.exception("Error inesperado descargando %s -> %s: %s", url, dest_path, e)
        return False


def extraer_datos_tabla(esquema: str, tabla: str) -> pd.DataFrame:
    """
    Conecta a la base de datos PostgreSQL usando SQLAlchemy y las variables de entorno,
    y extrae todos los datos de la tabla especificada en el esquema dado.

    Args:
        esquema (str): Nombre del esquema de la base de datos.
        tabla (str): Nombre de la tabla de la que extraer los datos.

    Returns:
        pd.DataFrame: DataFrame de pandas con los datos de la tabla,
                      donde las columnas corresponden a los nombres de las columnas de la tabla.

    Raises:
        Exception: Si hay un error de conexión o consulta, se registra y se relanza.
    """
    if not all([db_host, db_port, db_user, db_pass, db_name]):
        logger.error("Faltan variables de entorno para la conexión a la DB")
        raise ValueError("Configuración de DB incompleta")

    try:
        logger.info("Conectando a la DB para extraer datos de la tabla '%s.%s'", esquema, tabla)
        # Crear la URL de conexión para PostgreSQL
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_url)
        
        query = f"SELECT * FROM {esquema}.{tabla}"
        df = pd.read_sql_query(query, engine)
        logger.info("Extraídos %d registros de la tabla '%s.%s'", len(df), esquema, tabla)
        return df
    except Exception as e:
        logger.exception("Error al extraer datos de '%s.%s': %s", esquema, tabla, e)
        raise
   
def obtener_debarred_firms(api_url: str, nombre_fuente: str, dest_dir: str) -> bool:
    """
    Extrae tablas HTML de una URL y las guarda como CSV.
    
    Args:
        api_url (str): URL de la página que contiene la(s) tabla(s) a extraer.
        nombre_fuente (str): Nombre identificador de la fuente de datos (usado para el nombre del archivo).
        dest_dir (str): Directorio donde guardar el archivo CSV.
    
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario.
    """
    try:
        # Asegurar que el directorio destino existe
        os.makedirs(dest_dir, exist_ok=True)
        
        logger.info("Extrayendo tablas de la fuente '%s' desde: %s", nombre_fuente, api_url)
        
        # Extraer todas las tablas de la página
        tables = pd.read_html(api_url)
        
        if not tables:
            logger.error("No se encontraron tablas en %s", api_url)
            return False
        
        logger.info("Se encontraron %d tabla(s) en la página de '%s'", len(tables), nombre_fuente)
        
        # Si hay múltiples tablas, guardar la primera (principal)
        df = tables[0]
        
        # Guardar como CSV
        csv_path = os.path.join(dest_dir, f"{nombre_fuente}.csv")
        df.to_csv(csv_path, index=False)
        logger.info("Tabla de '%s' guardada exitosamente en %s (%d registros)", 
                    nombre_fuente, csv_path, len(df))
        
        return True

    except requests.exceptions.RequestException as e:
        logger.exception("Error de requests al extraer tabla de '%s': %s", nombre_fuente, e)
        return False
    except ValueError as e:
        logger.exception("Error extrayendo tablas de '%s': %s", nombre_fuente, e)
        return False
    except Exception as e:
        logger.exception("Error inesperado al extraer datos de '%s': %s", nombre_fuente, e)
        return False

def obtener_debarred_firms_rpa(url: str, nombre_fuente: str, dest_dir: str) -> bool:
    """
    Extrae datos usando RPA (Playwright) de una URL y guarda como XLSX.
    
    Args:
        api_url (str): URL de la página para RPA.
        nombre_fuente (str): Nombre identificador de la fuente de datos (usado para el nombre del archivo).
        dest_dir (str): Directorio donde guardar el archivo XLSX.
    
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario.
    """
    try:
        # Asegurar que el directorio destino existe
        os.makedirs(dest_dir, exist_ok=True)
        
        logger.info("Ejecutando RPA para la fuente '%s' desde: %s", nombre_fuente, url)
        
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto(url)
            with page.expect_download() as download_info:
                page.get_by_role("button", name="").click()
            download = download_info.value
            file_path = os.path.join(dest_dir, f"{nombre_fuente}_rpa.xlsx")
            download.save_as(file_path)
            logger.info("Archivo RPA de '%s' guardado exitosamente en %s", nombre_fuente, file_path)
            
            context.close()
            browser.close()
        
        return True

    except TimeoutError as e:
        logger.exception("Timeout en RPA para '%s': %s", nombre_fuente, e)
        return False
    except Exception as e:
        logger.exception("Error inesperado en RPA para '%s': %s", nombre_fuente, e)
        return False