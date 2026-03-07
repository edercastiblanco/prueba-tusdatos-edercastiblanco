import os
from numpy import record
import pandas as pd
import logging
import json
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

def normalizar_fecha_iso(fecha_sn):
    """
    Normaliza una fecha a formato ISO 8601 (YYYY-MM-DD).
    Maneja strings, datetime, NaN, etc.
    """
    if pd.isna(fecha_sn) or fecha_sn == '' or fecha_sn is None:
        return None
    try:
        dt = pd.to_datetime(fecha_sn, errors='coerce')
        if pd.isna(dt):
            return None
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return None

def xml_un_to_df():
    tree = ET.parse('data/raw/UN.xml')
    root = tree.getroot()
    
    # --- PROCESAMIENTO DE INDIVIDUOS ---
    individuals_data = []
    for ind in root.findall('.//INDIVIDUAL'):
        row = {
            'fuente' : 'UN',
            'tipo_sujeto': 'PERSONA_NATURAL',
            'nombres': f"{ind.findtext('FIRST_NAME', '')}".strip(),
            'apellidos': f"{ind.findtext('SECOND_NAME', '')} {ind.findtext('THIRD_NAME', '')}".strip(),
            'aliases': "; ".join([a.findtext('ALIAS_NAME') for a in ind.findall('./INDIVIDUAL_ALIAS') if a.findtext('ALIAS_NAME')]),
            'fecha_nacimiento': "; ".join([f"{d.findtext('DATE', '')}{d.findtext('YEAR', '')}" 
                                          for d in ind.findall('./INDIVIDUAL_DATE_OF_BIRTH')]),
            'nacionalidad': "; ".join([v.text for v in ind.findall('./NATIONALITY/VALUE') if v.text]),
            'numero_documento': ind.findtext('DATAID'),
            'tipo_sancion': ind.findtext('COMMENTS1'),
            'fecha_sancion': ind.findtext('LISTED_ON'),
            'fecha_vencimiento': '',
            'activo':True,
            'id_referencia': ind.findtext('DATAID')
        }
        individuals_data.append(row)

    # --- PROCESAMIENTO DE ENTIDADES ---
    entities_data = []
    for ent in root.findall('.//ENTITY'):
        paises_unicos = set(addr.findtext('COUNTRY') for addr in ent.findall('./ENTITY_ADDRESS') if addr.findtext('COUNTRY'))
        nacionalidad_str = "; ".join(paises_unicos) if paises_unicos else ''

        row = {
            'fuente' : 'UN',
            'tipo_sujeto': 'PERSONA_JURIDICA',
            'nombres': ent.findtext('FIRST_NAME'),
            'apellidos': '',
            'aliases': "; ".join([a.findtext('ALIAS_NAME') for a in ent.findall('./ENTITY_ALIAS') if a.findtext('ALIAS_NAME')]),
            'fecha_nacimiento': '',
            'nacionalidad': nacionalidad_str,
            'numero_documento': ent.findtext('DATAID'),
            'tipo_sancion': ent.findtext('COMMENTS1'),
            'fecha_sancion': ent.findtext('LISTED_ON'),
            'fecha_vencimiento': '',
            'activo':True,
            'id_referencia': ent.findtext('DATAID'),
        }
        entities_data.append(row)

    df = pd.DataFrame(individuals_data + entities_data)
    # Normalizar fechas a ISO 8601
    date_columns = ['fecha_sancion', 'fecha_nacimiento']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_fecha_iso)
    return df

def xml_eu_to_df():
    # Definir el espacio de nombres (namespace) del XML
    ns = {'fsd': 'http://eu.europa.ec/fpi/fsd/export'}
    
    data = []
    
    # iterparse permite procesar el archivo elemento por elemento sin cargarlo todo en RAM
    context = ET.iterparse('data/raw/EU_DRIVE.xml', events=('start', 'end'))
    
    # Buscamos el elemento raíz para poder limpiar la memoria de sus hijos
    event, root = next(context)
    
    for event, elem in context:
        # Solo procesamos cuando se termina de leer una etiqueta 'sanctionEntity'
        if event == 'end' and elem.tag == '{http://eu.europa.ec/fpi/fsd/export}sanctionEntity':
            record = {}
            
            # Atributos base
            #record.update({f"entity_{k}": v for k, v in elem.attrib.items()})
            record['fecha_sancion'] = elem.get('designationDate', '')
            #record['designation_details'] = elem.get('designationDetails', '')
            #record['united_nation_id'] = elem.get('unitedNationId', '')
            record['numero_documento'] = elem.get('euReferenceNumber', '')
            record['id_referencia'] = elem.get('logicalId', '')
           
            # SubjectType
            sub = elem.find('fsd:subjectType', ns)
            if sub is not None:
                subject_code = sub.get('code', '').strip().lower()
                record['tipo_sujeto'] = 'PERSONA_NATURAL' if subject_code == 'person' else 'PERSONA_JURIDICA'
                #record['subject_class'] = sub.get('classificationCode', '')
            
            # Regulation
            reg = elem.find('fsd:regulation', ns)
            if reg is not None:
                #record['reg_number'] = reg.get('numberTitle', '')
                #record['reg_prog'] = reg.get('programme', '')
                record['fecha_vencimiento'] = ''

                url_node = reg.find('fsd:publicationUrl', ns)
                record['tipo_sancion'] = url_node.text if url_node is not None else ''

            # Nombres (Uso de listas por comprensión para mayor velocidad)
            names = [
                f"{n.get('wholeName', '')}".strip(" ()") 
                for n in elem.findall('fsd:nameAlias', ns)
            ]
            record['aliases'] = " | ".join(names)
            
            gen = elem.find('fsd:nameAlias', ns)
            if gen is not None:
                record['firstName'] = gen.get('firstName', '').strip()
                record['middleName'] = gen.get('middleName', '').strip()
                record['nombres'] = f"{record['firstName']} {record['middleName']}".strip()
                record['apellidos'] = gen.get('lastName', '')

            # Identificaciones (Pasaportes, IDs, etc.)
            ids = [
                f"{i.get('typeDescription', '')}: {i.get('number', '')} ({i.get('countryDescription', '')})"
                for i in elem.findall('fsd:identification', ns)
            ]
            record['identifications'] = " | ".join(ids)

            # 8. Cumpleaños
            birthdates = elem.find('fsd:birthdate', ns)
            if birthdates is not None:
                record['fecha_nacimiento'] = birthdates.get('birthdate', '')
                record['nacionalidad'] = birthdates.get('countryIso2Code', '')
                record['fuente'] = 'EU'
                record['activo'] = True

            data.append(record)
            
            # CRÍTICO PARA OPTIMIZACIÓN: 
            # Elimina el elemento procesado de la memoria para mantener el consumo bajo
            elem.clear()
            root.clear() # Limpia las referencias en el nodo raíz también
            

    df = pd.DataFrame(data)
    # Normalizar fechas a ISO 8601
    date_columns = ['fecha_sancion', 'fecha_nacimiento']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_fecha_iso)

    # Estandarizar columnas finales para EU
    columnas_finales = [
        'fuente',
        'tipo_sujeto',
        'nombres',
        'apellidos',
        'aliases',
        'fecha_nacimiento',
        'nacionalidad',
        'numero_documento',
        'tipo_sancion',
        'fecha_sancion',
        'fecha_vencimiento',
        'activo',
        'id_referencia',
    ]

    for col in columnas_finales:
        if col not in df.columns:
            df[col] = ''

    return df[columnas_finales]

def json_fcpa_to_df():
    file_path = 'data/raw/FCPA.json'
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error al cargar el archivo JSON: {e}")
        return None

    hits = data.get('hits', {}).get('hits', [])
    print(f"Registros encontrados: {len(hits)}")

    all_rows = []

    for hit in hits:
        source = hit.get('_source', {})
        
        id_archivo = hit.get('_id')
        periodo_finalizado = source.get('period_ending', 'N/A')
        fecha_archivo = source.get('file_date', '')
        tipo_form = source.get('form', 'Desconocido')
        descripcion = source.get('file_description', '')
        
        ciks = source.get('ciks', [])
        nombres = source.get('display_names', [])
        ubicaciones = source.get('biz_locations', [])
        inc_states = source.get('inc_states', [])
        
        if not ciks:
            ciks = [None]

        for i in range(len(ciks)):
            row = {
                'numero_documento': ciks[i],
                'id_archivo': id_archivo,
                'periodo_finalizado': periodo_finalizado,
                'fecha_sancion': fecha_archivo,
                'tipo_sancion': tipo_form,
                'descripcion_archivo': descripcion,
                'nombres': nombres[i] if i < len(nombres) else "",
                'ubicacion_especifica': ubicaciones[i] if i < len(ubicaciones) else "",
                'estado_incorporacion': inc_states[i] if i < len(inc_states) else "",
                'todos_los_sics': "; ".join(map(str, source.get('sics', []))),
                'todos_los_biz_states': "; ".join(source.get('biz_states', []))
            }
            all_rows.append(row)

    # Crear DataFrame temporal con filas repetidas
    df = pd.DataFrame(all_rows)

    # --- PROCESO DE CONSOLIDACIÓN (Para pasar de imagen 1 a imagen 2) ---
        
    df_final = df.groupby('numero_documento').agg({
        'id_archivo': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'descripcion_archivo': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'periodo_finalizado': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'fecha_sancion': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'tipo_sancion': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'nombres': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'ubicacion_especifica': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'estado_incorporacion': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'todos_los_sics': lambda x: "; ".join(sorted(set(map(str, x.dropna())))),
        'todos_los_biz_states': lambda x: "; ".join(sorted(set(map(str, x.dropna()))))
    }).reset_index()

    # Normalizar fechas a ISO 8601
    date_columns = ['periodo_finalizado', 'fecha_sancion']
    for col in date_columns:
        if col in df_final.columns:
            df_final[col] = df_final[col].apply(normalizar_fecha_iso)
    
    df_final['fuente'] = 'FCPA'
    return df_final

def txt_paco_disc_to_df():
    """
    Carga el archivo PACO_DISC.txt y lo convierte en un DataFrame procesado.
    
    Args:
        ruta_archivo (str): Ruta local o nombre del archivo .txt
        
    Returns:
        pd.DataFrame: DataFrame con la información organizada y limpia.
    """
    
    # 1. Definición de la estructura de columnas
    columnas = [
        "ID_REGISTRO", "TIPO_PROCESO", "SUJETO_TIPO", "COD_TIPO_DOC", "TIPO_DOCUMENTO",
        "DOCUMENTO", "APELLIDO_1", "APELLIDO_2", "NOMBRE_1", "NOMBRE_2",
        "CARGO", "DEP_HECHOS", "MUN_HECHOS", "SANCION", "DURACION_ANOS", 
        "DURACION_MESES", "DURACION_DIAS", "INSTANCIA", "AUTORIDAD_SANCIONA",
        "FECHA_PROVIDENCIA", "NUM_PROVIDENCIA", "ENTIDAD", "DEP_ENTIDAD", 
        "MUN_ENTIDAD", "ANIO_REG", "MES_REG", "DIA_REG", "RESUMEN_TIEMPO"
    ]
    file_path = 'data/raw/PACO_DISC.txt'
    try:
        # 2. Intento de carga con manejo de codificación (común en archivos del gobierno)
        try:
            df = pd.read_csv(file_path, names=columnas, header=None, 
                             quotechar='"', sep=',', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, names=columnas, header=None, 
                             quotechar='"', sep=',', encoding='latin-1')

        # 3. Limpieza y Formateo
        # Convertir fecha a objeto datetime real
        df['FECHA_PROVIDENCIA'] = pd.to_datetime(df['FECHA_PROVIDENCIA'], errors='coerce')
        
        # Llenar valores nulos en duraciones numéricas con 0
        columnas_numericas = ['DURACION_ANOS', 'DURACION_MESES', 'DURACION_DIAS']
        df[columnas_numericas] = df[columnas_numericas].fillna(0)
        
        # Eliminar posibles espacios en blanco en columnas de texto
        columnas_texto = df.select_dtypes(include=['object']).columns
        df[columnas_texto] = df[columnas_texto].apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Normalizar fecha a ISO 8601
        df['FECHA_PROVIDENCIA'] = df['FECHA_PROVIDENCIA'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
        df['tipo_sujeto'] = 'PERSONA_NATURAL'
        df['nacionalidad'] = 'CO'
        df['fuente'] = 'PACO_DISC'
        
        # Unir apellidos y nombres
        df['apellidos'] = (df['APELLIDO_1'].fillna('') + ' ' + df['APELLIDO_2'].fillna('')).str.strip()
        df['nombres'] = (df['NOMBRE_1'].fillna('') + ' ' + df['NOMBRE_2'].fillna('')).str.strip()
        df.rename(columns={'DOCUMENTO': 'numero_documento'}, inplace=True)
        df.rename(columns={'SANCION': 'tipo_sancion'}, inplace=True)
        df.rename(columns={'FECHA_PROVIDENCIA': 'fecha_sancion'}, inplace=True)
        return df

    except FileNotFoundError:
        print("Error: El archivo no fue encontrado. Verifica la ruta.")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return None

def csv_paco_penal_to_df():
    # 1. Cargar el DataFrame
    file_path = 'data/raw/PACO_PENAL.csv'

    try:
        df = pd.read_csv(file_path)
        print(f"Archivo '{file_path}' cargado exitosamente.")
    except Exception as e:
        print(f"Error al cargar el archivo: {e}")
        return None

    # 2. Limpieza básica de datos
    # Eliminar espacios en blanco accidentales en columnas de texto
    columnas_texto = df.select_dtypes(include=['object']).columns
    for col in columnas_texto:
        df[col] = df[col].str.strip()
    
    return df

def xml_ofac_to_df():
    # Namespace oficial del archivo
    ns = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/ENHANCED_XML'}
    
    tree = ET.parse('data/raw/OFAC.xml')
    root = tree.getroot()
    
    entities, names, addresses, features, ids = [], [], [], [], []

    for entity in root.findall('.//ns:entity', ns):
        ent_id = entity.get('id')
        ent_type = entity.findtext('.//ns:entityType', default=None, namespaces=ns)
        
        # 1. Información General y Programas
        prog_list = [p.text for p in entity.findall('.//ns:sanctionsProgram', ns)]
        entities.append({
            'entity_id': ent_id,
            'type': ent_type,
            'programs': '; '.join(prog_list)
        })

        # 2. Nombres y Alias (A.K.A.)
        for name in entity.findall('.//ns:names/ns:name', ns):
            is_pri = name.findtext('ns:isPrimary', namespaces=ns)
            full_n = name.findtext('.//ns:formattedFullName', namespaces=ns)
            names.append({'entity_id': ent_id, 'is_primary': is_pri, 'full_name': full_n})

        # 3. Direcciones
        for addr in entity.findall('.//ns:addresses/ns:address', ns):
            country = addr.findtext('ns:country', namespaces=ns)
            full_addr = addr.findtext('.//ns:formattedAddress', namespaces=ns)
            addresses.append({'entity_id': ent_id, 'country': country, 'full_address': full_addr})

        # 4. Características (Fechas, Género, etc.)
        for feat in entity.findall('.//ns:features/ns:feature', ns):
            f_type = feat.findtext('ns:type', namespaces=ns)
            f_val = feat.findtext('ns:value', namespaces=ns)
            features.append({'entity_id': ent_id, 'feature': f_type, 'value': f_val})

        # 5. Documentos de Identidad
        for doc in entity.findall('.//ns:identityDocuments/ns:identityDocument', ns):
            d_type = doc.findtext('ns:type', namespaces=ns)
            d_num = doc.findtext('ns:documentNumber', namespaces=ns)
            d_country = doc.findtext('ns:issuingCountry', namespaces=ns)
            ids.append({'entity_id': ent_id, 'id_type': d_type, 'id_number': d_num, 'country': d_country})

    # Crear DataFrames
    df_ent, df_names, df_addr, df_feat, df_ids = pd.DataFrame(entities), pd.DataFrame(names), pd.DataFrame(addresses), pd.DataFrame(features), pd.DataFrame(ids)
    
    # Normalizar fechas en features de OFAC
    if not df_feat.empty:
        df_feat['value'] = df_feat.apply(lambda row: normalizar_fecha_iso(row['value']) if 'date' in row['feature'].lower() or 'birth' in row['feature'].lower() else row['value'], axis=1)
    
    # Unificar en un solo DataFrame expandido
    # Empezar con df_ids
    df_unified = df_ids.copy()
    
    # Merge con entities
    df_unified = df_unified.merge(df_ent, on='entity_id', how='left')
    
    # Merge con names (expandirá filas si hay múltiples names por entity_id)
    if not df_names.empty:
        df_unified = df_unified.merge(df_names, on='entity_id', how='left')
    
    # Merge con addresses
    if not df_addr.empty:
        df_unified = df_unified.merge(df_addr, on='entity_id', how='left')
    
    # Merge con features
    if not df_feat.empty:
        df_feat_pivot = df_feat.pivot_table(index='entity_id', columns='feature', values='value', aggfunc=lambda x: x.iloc[0] if len(x) > 0 else None)
        df_feat_pivot.reset_index(inplace=True)
        df_unified = df_unified.merge(df_feat_pivot, on='entity_id', how='left')
    
    # Crear columnas nombres y aliases basada en is_primary
    if not df_names.empty:
        nombres_dict = {}
        aliases_dict = {}
        for ent_id, group in df_unified.groupby('entity_id'):
            primary = group[group['is_primary'] == 'true']['full_name']
            non_primary = group[group['is_primary'] != 'true']['full_name']
            nombres_dict[ent_id] = primary.iloc[0] if not primary.empty else None
            aliases_dict[ent_id] = '; '.join(set(non_primary.dropna())) if not non_primary.empty else None
        df_unified['nombre_completo'] = df_unified['entity_id'].map(nombres_dict)
        df_unified['aliases'] = df_unified['entity_id'].map(aliases_dict)
        # Partir nombres en apellidos y nombres solo para PERSONA_NATURAL
        df_unified['apellidos'] = df_unified.apply(lambda row: row['nombre_completo'].split(', ')[0] if row['type'] == 'Individual' and row['nombre_completo'] else None, axis=1)
        df_unified['nombres'] = df_unified.apply(lambda row: row['nombre_completo'].split(', ')[1] if row['type'] == 'Individual' and row['nombre_completo'] else row['nombre_completo'], axis=1)
        df_unified['fuente'] = 'OFAC'
    
    # Filtrar solo Entity e Individual, y mapear type
    df_unified = df_unified[df_unified['type'].isin(["Entity", "Individual"])]
    df_unified['type'] = df_unified['type'].map({'Entity': 'PERSONA_JURIDICA', 'Individual': 'PERSONA_NATURAL'})

    # renombrar columnas para estandarizar con otras fuentes
    df_unified.rename(columns={'type': 'tipo_sujeto'}, inplace=True)
    df_unified.rename(columns={'country': 'nacionalidad'}, inplace=True)
    df_unified.rename(columns={'id_number': 'numero_documento'}, inplace=True)
    df_unified.rename(columns={'country_x': 'nacionalidad'}, inplace=True)
    df_unified.rename(columns={'Additional Sanctions Information -': 'tipo_sancion'}, inplace=True)

    # Eliminar columnas innecesarias
    df_unified = df_unified.drop(columns=['is_primary', 'full_name', "full_address", "Aircraft Construction Number (also called L/N or S/N or F/N)",
                                            "Aircraft Manufacture Date", "Aircraft Manufacturer's Serial Number (MSN)", "Aircraft Model", "Aircraft Operator",
                                            "Aircraft Tail Number", "Aircraft Type","Digital Currency Address - ARB", "Digital Currency Address - BCH", 
                                            "Digital Currency Address - BSC", "Digital Currency Address - BSV", "Digital Currency Address - BTG", "Digital Currency Address - DASH", 
                                            "Digital Currency Address - ETC", "Digital Currency Address - ETH", "Digital Currency Address - LTC", "Digital Currency Address - SOL", 
                                            "Digital Currency Address - TRX", "Digital Currency Address - USDC", "Digital Currency Address - USDT", "Digital Currency Address - XBT", 
                                            "Digital Currency Address - XMR", "Digital Currency Address - XRP", "Digital Currency Address - XVG", "Digital Currency Address - ZEC",
                                            "Email Address", "Equity Ticker","Executive Order 13662 Directive Determination -","Executive Order 13846 information:", 
                                            "Executive Order 14024 Directive Information", "Executive Order 14024 Directive Information -", "Former Vessel Flag", "Vessel Name", 
                                            "Vessel Flag", "Vessel Owner", "Aircraft Mode S Transponder Code","Vessel Type","IFCA Determination -", "Target Type", 
                                            "VESSEL TYPE","Vessel Call Sign", "Vessel Gross Registered Tonnage","Vessel Tonnage","Vessel Year of Build","Website", "SWIFT/BIC","country_y",
                                            "MICEX Code","Organization Established Date", "Organization Type:", "Other Vessel Call Sign","Other Vessel Flag","Other Vessel Type",
                                            "Phone Number","Previous Aircraft Tail Number", "Registration Country","UN/LOCODE","id_type","D-U-N-S Number","Effective Date (EO 14024 Directive 2):",
                                            "Effective Date (EO 14024 Directive 3):","ISIN","Nationality Country","Nationality of Registration","PAIPA Section 2 Information:",
                                            "PEESA Information:","BIK (RU)", "CAATSA Section 235 Information:", "Citizenship Country", "Gender", "Listing Date (EO 14024 Directive 2):", 
                                            "Listing Date (EO 14024 Directive 3):", "Secondary sanctions risk:", 
                                            "Transactions Prohibited For Persons Owned or Controlled By U.S. Financial Institutions:"], errors='ignore')
    
    # Eliminar duplicados por entity_id
    df_unified = df_unified.drop_duplicates()
        
    return df_unified

def xlsx_banco_mundial_to_df():
    """
    Carga el archivo del Banco Mundial, renombra las columnas especificadas
    y limpia el formato de encabezados múltiples.
    """
    # 1. Leer el archivo Excel saltando la primera fila de metadatos (Downloaded on...)
    df = pd.read_excel('data/raw/WORLD_BANK_rpa.xlsx', skiprows=1)

    # 2. Definir el mapeo de columnas según tu requerimiento
    # Nota: 'Ineligibility Period' contiene la fecha de inicio (From Date)
    # y 'Unnamed: 5' contiene la fecha de fin (To Date).
    columnas_map = {
        'Firm Name': 'nombres',
        'Country': 'nacionalidad',
        'Ineligibility Period': 'fecha_sancion',
        'Unnamed: 5': 'fecha_vencimiento',
        'Grounds': 'tipo_sancion'
    }

    # 3. Renombrar y seleccionar solo las columnas deseadas
    df_transformado = df.rename(columns=columnas_map)
    columnas_finales = ['nombres', 'nacionalidad', 'fecha_sancion', 'fecha_vencimiento', 'tipo_sancion']
    df_transformado = df_transformado[columnas_finales]

    # 4. Eliminar la fila que contiene el texto de los sub-encabezados ("From Date", "To Date")
    # Esta fila suele ser la primera después de cargar con skiprows=1
    df_final = df_transformado.iloc[1:].reset_index(drop=True)

    # 5. Normalizar fechas y etiquetar fuente
    for col in ['fecha_sancion', 'fecha_vencimiento']:
        if col in df_final.columns:
            df_final[col] = df_final[col].apply(normalizar_fecha_iso)
    df_final['fuente'] = 'WORLD_BANK'

    return df_final