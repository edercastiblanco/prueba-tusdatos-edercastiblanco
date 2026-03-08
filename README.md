# prueba-tusdatos-edercastiblanco

Pipeline de ingesta y normalizacion de listas de sanciones/antecedentes.

## 1) Requisitos previos

Antes de instalar dependencias, en el equipo destino debes tener:

- Python `3.11.x` (recomendado: `3.11.9` o similar)
- `pip` actualizado
- PostgreSQL (recomendado 14+) la copia de seguridad se encuentra en la carpeta data/bd/tusdatosco-edercastiblanco.sql
- Acceso de red a las fuentes externas (OFAC, UN, EU, SEC, etc.)
- Se debe renombrar el copia.env.test a .env

Adicional a `requirements.txt`:

- Playwright necesita instalar navegadores del sistema despues del `pip install`

## 2) Clonar y crear entorno virtual

En Windows (PowerShell):

```powershell
git clone <URL_DEL_REPOSITORIO>
cd prueba-tusdatos-edercastiblanco
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

En Linux/macOS (bash):

```bash
git clone <URL_DEL_REPOSITORIO>
cd prueba-tusdatos-edercastiblanco
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

## 3) Instalar dependencias

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Si Playwright falla en Linux por librerias del sistema, ejecutar:

```bash
python -m playwright install-deps chromium
```

## 4) Configurar variables de entorno

Puedes usar `.env` o `.env.<entorno>` (ejemplo: `.env.local`).

Variables necesarias:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=tu_password
DB_NAME=tusdatosco
```

Variables opcionales:

```env
NORMALIZACION_MAX_WORKERS=4
CARGA_INCREMENTAL=true
LOG_LEVEL=INFO
LOG_DIR=logs
```

## 5) Configuracion minima de base de datos

Este proyecto espera:

1. Schema `bronze` para tablas normalizadas.
2. Tabla `configuracion.fuentes` con las fuentes a ingerir.

DDL base sugerido:

```sql
CREATE SCHEMA IF NOT EXISTS configuracion;
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS configuracion.fuentes (
	alias TEXT NOT NULL,
	url TEXT NOT NULL,
	tipo TEXT NOT NULL,
	formato TEXT NOT NULL
);
```

Notas:

- El schema `monitoreo` y la tabla `monitoreo.metricas` se crean automaticamente durante la corrida.
- La carga incremental soporta evolucion de esquema: si aparecen columnas nuevas en una fuente, intenta agregarlas automaticamente en la tabla destino.

## 6) Ejecutar el pipeline

Desde la raiz del repositorio:

```bash
python run_pipeline.py --env local
```

Alternativa (orquestador directo):

```bash
python pipeline/run_pipeline.py
```

Salidas generadas:

- Archivos descargados: `data/raw/`
- Archivos normalizados: `data/normalizado/`
- Metricas JSON por corrida: `pipeline/monitoreo/metricas/<run_id>.json`
- Logs: `logs/app.log`

## 7) Ejecutar pruebas

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## 8) Problemas comunes

- `UndefinedColumn` en incremental:
	- La logica actual agrega columnas nuevas automaticamente. Verifica permisos de `ALTER TABLE` del usuario DB.
- `No se encontro .env.local ni .env`:
	- Crea `.env` o ejecuta con `--env` apuntando a un archivo existente (`.env.<nombre>`).
- Error en Playwright:
	- Reinstala navegadores con `python -m playwright install chromium`.

## 9) Estructura principal

- `run_pipeline.py`: wrapper de entorno y entrypoint desde raiz
- `pipeline/run_pipeline.py`: orquestador de etapas
- `pipeline/fuentes/`: descarga e ingesta
- `pipeline/normailzacion/`: normalizacion y carga incremental
- `pipeline/monitoreo/`: persistencia de metricas
- `pipeline/db/`: conexion SQLAlchemy
- `tests/`: pruebas minimas de regresion
