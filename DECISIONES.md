# Explicacion General Del Repositorio

## Objetivo
Este repositorio implementa un pipeline de datos para listas de sanciones y antecedentes.

Capacidades principales:
1. Ingesta multi-fuente en paralelo.
2. Normalizacion por fuente a un esquema de negocio comun.
3. Persistencia en Excel y PostgreSQL (`bronze`).
4. Monitoreo de ejecucion en PostgreSQL (`monitoreo.metricas`) y JSON por corrida.
5. Carga incremental por hash para detectar nuevos, modificados y eliminados.

## Estructura Del Proyecto

- `run_pipeline.py`: entrypoint desde raiz con `--env`.
- `pipeline/run_pipeline.py`: orquestador principal (ingesta + normalizacion + metricas).
- `pipeline/fuentes/`: extraccion e ingesta.
- `pipeline/normailzacion/`: transformaciones y carga incremental.
- `pipeline/monitoreo/`: persistencia de metricas.
- `pipeline/db/`: conexion a PostgreSQL.
- `data/raw/`: archivos descargados.
- `data/normalizado/`: salidas Excel de cada fuente.
- `logs/`: logs de ejecucion.
- `tests/`: pruebas automatizadas minimas (Realizadas por IA).

## Flujo De Ejecucion

1. `run_pipeline.py` (raiz) carga variables de entorno segun `--env` y ejecuta `pipeline/run_pipeline.py`.
2. Se ejecuta ingesta en paralelo (`pipeline/fuentes/ingesta_fuentes.py`).
3. Se ejecutan normalizaciones en paralelo (`pipeline/normailzacion/ejecucion_normalizacion.py`).
4. Cada resultado se guarda en Excel y en PostgreSQL usando carga incremental (`pipeline/normailzacion/carga_incremental.py`).
5. Se persisten metricas en `monitoreo.metricas` y en `pipeline/monitoreo/metricas/{run_id}.json`.

## Modulos Clave

### Ingesta

Archivo: `pipeline/fuentes/ingesta_fuentes.py`

- Lee fuentes desde `configuracion.fuentes`.
- Procesa fuentes en paralelo con `ThreadPoolExecutor`.
- Soporta descarga por URL, ZIP y RPA (Playwright) para Banco Mundial.
- Retorna metricas por fuente (`estado`, `duracion_segundos`, `error`).

Archivo: `pipeline/fuentes/extraccion.py`

- Descarga archivos con `requests` y reintentos (`urllib3 Retry`).
- Incluye utilidades para extraccion de tablas y ejecucion RPA.

### Normalizacion

Archivo: `pipeline/normailzacion/normalizacion.py`

- Contiene funciones por fuente:
  - `xml_un_to_df()`
  - `xml_eu_to_df()`
  - `json_fcpa_to_df()`
  - `txt_paco_disc_to_df()`
  - `csv_paco_penal_to_df()`
  - `xml_ofac_to_df()`
  - `xlsx_banco_mundial_to_df()`
- Estandariza fechas a ISO 8601 (`YYYY-MM-DD`) y homogeniza columnas de negocio.
- Aplica reglas especificas por fuente (tipos de sujeto, alias, documentos, etc.).

Archivo: `pipeline/normailzacion/ejecucion_normalizacion.py`

- Define el plan `tareas_normalizacion`.
- Ejecuta tareas en paralelo.
- Registra metricas por cada normalizacion.

Archivo: `pipeline/normailzacion/carga_incremental.py`

- Genera `hash_clave` (identidad) y `hash_contenido` (contenido).
- Detecta:
  - nuevos
  - modificados
  - eliminados
- Maneja estados historicos:
  - `ACTIVO`
  - `REEMPLAZADO`
  - `ELIMINADO`
- Excluye columnas tecnicas del hash para evitar falsos positivos.
- Deduplica `hash_clave` para evitar errores por indices duplicados.
- Soporta evolucion de esquema: si llegan columnas nuevas en una tabla existente, las agrega automaticamente antes del `append`.

#### Que Pasa Si Cambia El Esquema De Una Fuente

Cuando una normalizacion empieza a devolver columnas nuevas (por ejemplo, en `EU`), la carga incremental evita fallar por columnas inexistentes en PostgreSQL.

Comportamiento actual:
1. Detecta las columnas del `DataFrame` que no existen en la tabla destino.
2. Ejecuta `ALTER TABLE ... ADD COLUMN` por cada columna faltante.
3. Mapea tipos de pandas a SQL de forma automatica:
  - `bool` -> `BOOLEAN`
  - enteros -> `BIGINT`
  - decimales -> `DOUBLE PRECISION`
  - fechas/datetime -> `TIMESTAMP`
  - otros -> `TEXT`
4. Registra un warning en logs por cada columna agregada.
5. Continua la insercion incremental sin detener el pipeline.

Alcance y limites:
- Agrega columnas nuevas, pero no elimina columnas antiguas.
- No renombra columnas automaticamente.
- Si cambia el nombre de una columna, se tratara como "columna nueva" y la anterior permanecera en la tabla.
- Si hay cambios de tipo complejos, se prioriza robustez operativa (agregar y continuar); los ajustes finos de tipo se pueden hacer luego por migracion controlada.

Ejemplo real aplicado:
- En `bronze.eu` se agregaron automaticamente columnas nuevas como `id_registro` y `fecha_ingesta`, evitando el error `UndefinedColumn` durante `INSERT`.

### Monitoreo

Archivo: `pipeline/monitoreo/metricas.py`

- Crea `monitoreo.metricas` si no existe.
- Inserta metricas de ingesta y normalizacion.
- Exporta JSON por corrida en `pipeline/monitoreo/metricas/{run_id}.json`.

### Conexion DB

Archivo: `pipeline/db/conexion.py`

- Centraliza la creacion del engine SQLAlchemy a partir de variables de entorno.

## Pruebas Automatizadas

Archivo: `tests/test_normalizacion_minima.py`

Cobertura minima para prevenir regresiones recientes:
- `normalizar_fecha_iso`.
- Salida EU (`xml_eu_to_df`) validando esquema de negocio requerido y mapeo de `tipo_sujeto`.
- Retorno de FCPA (`json_fcpa_to_df`) con campos criticos.

Ejecucion de pruebas:

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Base De Datos

- Schema `bronze`: tablas normalizadas por fuente.
- Schema `monitoreo`: tabla `metricas` con trazabilidad de ejecucion.

## Variables De Entorno

Conexion PostgreSQL:
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

Configuracion de ejecucion:
- `NORMALIZACION_MAX_WORKERS`: numero de hilos para normalizacion.
- `CARGA_INCREMENTAL`: `true`/`false` para activar incremental.
- `LOG_LEVEL` y `LOG_DIR`: configuracion de logs.

## Dependencias

Definidas en `requirements.txt`:
- `requests`
- `python-dotenv`
- `urllib3`
- `sqlalchemy`
- `pandas`
- `pycountry`
- `openpyxl`
- `numpy`
- `playwright`
- `psycopg2-binary`

## Comandos De Ejecucion

Ejecucion completa del pipeline (desde raiz):

```bash
python run_pipeline.py --env local
```

Ejecucion directa del orquestador:

```bash
python pipeline/run_pipeline.py
```
## ENTREGA

Empiezo por decir que ya estos años en cargos estrategicos me oxidaron para el desarrollo, por lo cual no terminé la prueba, es más no pude realizar el motor...
de igual manera me pareció una prueba muy interesante y sin tener contexto de los archivos o el cómo funciona toda la parte de validación lo hace un poco más complejo,
gaste buen tiempo estudiando las fuentes y tratando de obtener los tokens de las api para poder descargar los archivos sin problema...

1. Decisiones
- Para la ingesta de fuentes, se optó por un enfoque de paralelismo con `ThreadPoolExecutor` para mejorar tiempos, dado que muchas operaciones son I/O bound (descargas, lectura de archivos).
- Para la normalización, se definieron funciones específicas por fuente para manejar las particularidades de cada una, pero con un esquema de salida común.
- Para la persistencia, se implementó una carga incremental basada en hashes para detectar nuevos, modificados y eliminados sin necesidad de truncar tablas.
- Se decidió registrar metricas tanto en PostgreSQL como en archivos JSON por corrida para facilitar el monitoreo y la trazabilidad.
- Se usó el archivo compartido en el drive para la fuente EU ya que no me fue posible generar el token.
- Para la fuente del banco mundial decidí realizar la descarga con un RPA usando Playwright ya que la tabla siempre me salio sin paginar y me solicitava un token para consultarla.

2. Mejoras Pendientes
- Implementar el motor de reglas para aplicar transformaciones más complejas y reutilizables.
- Agregar pruebas automatizadas para cada función de normalización y carga incremental. (se realizaron unos test generales todos mayormente realizados con IA)
- Mejorar la gestion de errores y reintentos en la ingesta, especialmente para fuentes con APIs inestables.
- Implementar alertas o notificaciones en caso de fallos criticos en el pipeline.
- Optimizar la detección de cambios en la carga incremental para manejar casos de cambios masivos o esquemas muy dinámicos.

## PREGUNTAS
1. ¿Con qué frecuencia se actualiza cada fuente y cómo se detecta que hay una versión nueva?
Se recomienda realizar una extracción diaria, vi unas fuentes que se actualizan cada 3 horas por lo que supongo que es bueno realizarlo para esa fuente en el mismo tiempo.

2. ¿Cómo se re-ejecuta el matching solo sobre los registros que cambiaron sin reprocesar todo?
No llegue al punto, pero usaria una bandera para marcar los que cambiaron y de esa forma reprocesarlos.

3. ¿Cómo se notifica a los consumidores cuando un tercero que antes estaba limpio ahora aparece en una lista?
Si tenemos el listado de las consultas se puede generar un script donde se revise ese tercero x días atrás por quíen fue consultado y enviar un correo y un msj.

4. MATCHINF
    - ¿por qué eligió el algoritmo que usó? 
    No realicé el motor, pero leyendo escogeria el de Jaro-Winkler porque detecta de mejor manera las transposiciones, le da prioridad al inicio (donde uno se equivoca menos al digitar y maneja mejor las cadenas cortas)
    - ¿En qué escenario fallaría? 
    Penaliza mucho cuando tenemos por ejemplo Pedro Pablo y buscamos Pablo Pedro porque lo inicial no coincide.
    EN los alias se tendria puntuación baja
    - ¿Cómo escalaría el matching si la base de terceros fuera de 10 millones de registros en lugar de 10.000?
    Usaria elasticsearch.
    - ¿Cómo maneja ese cambio sin romper el pipeline ni perder datos históricos?
    Se puede implementar un proceso de migración donde se re-calculan los hashes de contenido con el nuevo algoritmo y se comparan con los anteriores para marcar los registros que cambiaron su estado (ej: de ACTIVO a REEMPLAZADO) y luego se actualizan progresivamente.
  4.1 Falsos positivos vs. falsos negativos: 
    - en un motor de matching para compliance, ¿cuál es más costoso?
    El mas costoso es el falso negativo ya que creo que tiene riesgos de sanciones para los clientes (a demás del reputacional para nosotros), por otro lado el falso positivo bloquearia una persona pero es más costo operacional.
    - ¿Cómo calibraría el umbral de similitud y qué proceso operacional diseñaría para manejar la zona gris?
    Con análisis de sensibilidad variando el umbral de 0.1 en 0.1 y ver que tal se comporta.
    Apoyarse en machine learning para ver si se puede generar un modelo o ya revisar si con IA se puede hacer algo (que es muy probable)
  4.2 Acceso a los datos: un analista externo solicita acceso completo a las listas normalizadas para un proyecto de investigación. Los datos son públicos en origen pero el pipeline agrega información adicional. ¿Cómo maneja el request?
    - Es sencillo no le daria acceso a la información, de manera amable le explico que por tema de habeas data no se puede compartir los datos asi sean publicos ya que nosotros los enriquecemos con diferentes fuentes. Obviamente un ente externo puede hacer ingenieria inversa y pues la idea es proteger el know how.
  4.3 Frecuencia vs. costo: OFAC puede actualizarse varias veces al día. ¿Cómo diseñaría el pipeline para balancear frescura de datos con costo operacional?
    - Para balancear frescura con costo, se puede implementar una estrategia de actualización incremental, donde se realicen extracciones frecuentes (ej: cada 6 horas) pero solo se procesen los cambios detectados desde la última extracción completa. Esto reduce el costo operacional al evitar reprocesar toda la fuente cada vez, mientras se mantiene una frescura razonable para los consumidores.
    


5. Estrategia de actualización
Para manejar actualizaciones de fuentes, se implementó una carga incremental basada en hashes. El proceso es el siguiente:
a. Generar `hash_clave` para cada registro basado en las columnas que definen su identidad (por ejemplo, número de documento).
b. Generar `hash_contenido` basado en el contenido completo del registro, excluyendo columnas técnicas como timestamps o IDs generados.
c. Comparar los hashes con los registros existentes en la tabla destino:
   - Si `hash_clave` no existe, es un nuevo registro (INSERT).
   - Si `hash_clave` existe pero `hash_contenido` es diferente, es un registro modificado (UPDATE).
   - Si un `hash_clave` existente no aparece en la nueva extracción, se marca como eliminado (UPDATE con estado `ELIMINADO`). 
d. Mantener un historial de estados para cada registro:
    - `ACTIVO`: registro vigente.
    - `REEMPLAZADO`: registro que fue modificado por otro nuevo.
    - `ELIMINADO`: registro que ya no aparece en la fuente.       
e. Excluir columnas técnicas del hash para evitar falsos positivos por cambios en timestamps o IDs generados.
f. Deduplicar `hash_clave` para evitar errores por índices duplicados en casos donde la fuente pueda tener registros repetidos.
g. Soportar evolución de esquema: si llegan columnas nuevas en una tabla existente, el proceso detecta las columnas faltantes y las agrega automáticamente antes de la inserción incremental, evitando errores por columnas inexistentes.

5. 

