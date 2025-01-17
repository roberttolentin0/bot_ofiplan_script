import json
import pyodbc
import psycopg2
import pandas as pd

# Definir los parámetros de conexión SQL SERVER
server_sql = '10.5.0.9'  # El nombre del servidor o la IP
database_sql = 'OFIPLAN'
username_sql = 'admin'
password_sql = 'Huaraz2022..*'

# Definir los parámetros de conexión PostgreSQL
server_postgresql = '40.86.9.189'  # El nombre del servidor o la IP
database_postgresql = 'RhDB2'
username_postgresql = 'sqladmin'
password_postgresql = 'Slayer20fer..'
port_postgresql = '5433'


# Codigo según OFIPLAN
ESTADO_CIVIL = {
    'CASADO': 'CAS',
    'CONVIVIENTE': 'CNV',
    'DIVORCIADO': 'DIV',
    'SOLTERO': 'SOL',
    'VIUDO': 'VIU',
}

# Codigo según OFIPLAN
GRADO_INSTRUCCION = {
    'DOCTORADO COMPLETO': '20',
    'DOCTORADO INCOMPLETO': '19',
    'ESPECIAL COMPLETA': '03',
    'ESPECIAL INCOMPLETA': '02',
    'GRADO DE BACHILLER': '14',
    'GRADO DE DOCTOR': '21',
    'GRADO DE MAESTRÍA': '18',
    'MAESTRIA COMPLETA': '17',
    'MAESTRIA INCOMPLETA': '16',
    'NO APLICABLE': 'NA',
    'PRIMARIA COMPLETA': '05',
    'PRIMARIA INCOMPLETA': '04',
    'SECUNDARIA INCOMPLETA': '06',  # SECUND. INCOMPLETA
    'SECUNDARIA COMPLETA': '07',
    'SIN EDUCACION FORMAL': '01',
    'SUPERIOR COMPLETA': '11',
    'SUPERIOR INCOMPLETA': '10',
    'TECNICA COMPLETA': '09',
    'TECNICA INCOMPLETA': '08',
    'TITULADO': '15',
    'UNIVERS. COMPLETA': '13',
    'UNIVERS. INCOMPLETA': '12'
}

# Mapas de abreviaturas a términos completos
ABREVIATURAS_VIAS = {
    'JR.': 'JIRON',
    'AV.': 'AVENIDA',
    'CARR.': 'CARRETERA',
    'PSJE.': 'PASAJE',
    'PLZA.': 'PLAZA',
    # Agregar más abreviaturas según sea necesario
}

ABREVIATURAS_ZONAS = {
    'URB.': 'URBANIZACION',
    'A.H.': 'ASENTAMIENTO HUMANO',
    'P.J.': 'PUEBLO JOVEN',
    'CAS.': 'CASERIO',
    # Agregar más abreviaturas según sea necesario
}

# Códigos OFIPLAN
TIPO_VIAS = {
    'ALAMEDA': '05',
    'AVENIDA': '01',
    'BAJADA': '15',
    'CALLE': '03',
    'CAMINO AFIRMADO': '21',
    'CAMINO RURAL': '14',
    'CARRETERA': '10',
    'GALERIA': '16',
    'JIRON': '02',
    'MALECON': '06',
    'OTROS': '99',
    'OVALO': '07',
    'PARQUE': '08',
    'PASAJE': '04',
    'PASEO': '18',
    'PLAZA': '09',
    'PLAZUELA': '19',
    'PORTAL': '20',
    'PROLONGACION': '17',
    'TROCHA': '13',
    'TROCHA CARROZABLE': '22'
}

# Códigos OFIPLAN
TIPO_ZONAS = {
    'ASENTAMIENTO HUMANO': '05',
    'BARRIO': '12',
    'CASERIO': '10',
    'CONJUNTO HABITACIONAL': '04',
    'COOPERATIVA': '06',
    'FUNDO': '11',
    'GRUPO': '09',
    'OTROS': '99',
    'PUEBLO JOVEN': '02',
    'RESIDENCIAL': '07',
    'UNIDAD VECINAL': '03',
    'URBANIZACION': '01',
    'ZONA INDUSTRIAL': '08'
}

# Códigos OFIPLAN
REGIMEN_PENSIONARIO = {
    'ONP': 'NA',  # *SNP
    'HABITAT': 'HAB',
    'HORIZONTE': 'HOR',
    'INTEGRA': 'INT',
    'PRIMA': 'PRI',
    'PROFUTURO': 'PRO',
    'SIN REG. PENSIONARIO': '99'
}


def get_trabajadores_ofiplan():
    # Crear la conexión SQL
    try:
        # Conectar a SQL Server (BD_OFIPLAN)
        conn_sql_server = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'  # Controlador ODBC
            f'SERVER={server_sql};'
            f'DATABASE={database_sql};'
            f'UID={username_sql};'
            f'PWD={password_sql};'
        )
        print("Conexión exitosa a SQL Server")
    except Exception as e:
        print("Error al conectar a SQL Server:", e)

    # Consulta para obtener los trabajadores de BD_OFIPLAN
    sql_query_ofiplan = """
        SELECT CO_TRAB, NO_APEL_PATE, NO_APEL_MATE, NO_TRAB
        FROM [OFIPLAN].[dbo].[TMTRAB_PERS]
    """
    df_ofiplan = pd.read_sql(sql_query_ofiplan, conn_sql_server)
    # print(df_ofiplan)
    conn_sql_server.close()
    return df_ofiplan


def get_trabajadores_rrhh():
    # Crear la conexión PostgreSQL
    try:
        # Conectar a SQL Server (BD_OFIPLAN)
        conn_postgres = psycopg2.connect(
            dbname=database_postgresql,
            user=username_postgresql,
            password=password_postgresql,
            host=server_postgresql,
            port=port_postgresql
        )
        print("Conexión exitosa a PostgreSQL")
    except Exception as e:
        print("Error al conectar a PostgreSQL:", e)

    # Consulta para obtener los trabajadores de BD_RRHH
    # idtypeemployee: 1 = PLANILLA
    sql_query_rrhh = """
    SELECT
        dni,
        fullname,
        lastname,
        birthdate,
        cellphone,
        email,
        address,
        idsite,
        (SELECT b.name FROM business b WHERE b.id = emp.idsite) AS departamento,
        idcharge,
        startdate,
        salary,
        sexo,
        condition,
        licence,
        emp.id_afp,
		(SELECT a.nombre FROM afp a WHERE a.id = emp.id_afp) AS regimen_pensionario,
        id_entidadfinanciera,
        cci,
        cargo,
        ubigeo
        FROM employee emp
	WHERE  emp.idtypeemployee = 1 AND status = true;
    """
    df_rrhh = pd.read_sql(sql_query_rrhh, conn_postgres)
    # print(df_rrhh)
    conn_postgres.close()

    return df_rrhh


def normalizar_direccion(direccion, abreviaturas):
    """Función para reemplazar abreviaturas con su equivalente completo"""
    for abrev, completo in abreviaturas.items():
        direccion = direccion.upper().replace(abrev, completo)
    return direccion

def obtener_codigo_via_y_zona(direccion):
    """Función para obtener los códigos"""
    direccion_normalizada_via = normalizar_direccion(direccion, ABREVIATURAS_VIAS)
    direccion_normalizada_zona = normalizar_direccion(direccion, ABREVIATURAS_ZONAS)

    cod_tipo_via = '99'  # Código por defecto para 'OTROS'
    cod_tipo_zona = '99'  # Código por defecto para 'OTROS'

    # Buscar coincidencias en tipo de vía
    for via in TIPO_VIAS:
        if via in direccion_normalizada_via:
            cod_tipo_via = TIPO_VIAS[via]
            break  # Salir después de encontrar la primera coincidencia

    # Buscar coincidencias en tipo de zona
    for zona in TIPO_ZONAS:
        if zona in direccion_normalizada_zona:
            cod_tipo_zona = TIPO_ZONAS[zona]
            break  # Salir después de encontrar la primera coincidencia

    return cod_tipo_via, cod_tipo_zona



def formatear_json(row):
    """Función para agregar los campos adicionales y formar el JSON"""
    # Dividir el apellido en paterno y materno
    apellidos = row['lastname'].split(" ", 1)
    ape_paterno = apellidos[0]
    ape_materno = apellidos[1] if len(apellidos) > 1 else ""

    # Formatear las fechas
    fecha_ingreso = row['startdate'].strftime(
        '%d/%m/%Y') if row['startdate'] else ""
    fecha_nacimiento = row['birthdate'].strftime(
        '%d/%m/%Y') if row['birthdate'] else ""

    direccion = row['address']
    cod_tipo_via, cod_tipo_zona = obtener_codigo_via_y_zona(direccion)
    regimen_pensionario = row["regimen_pensionario"].replace(" ","").split("-")[0]
    # Crear el JSON con los datos requeridos
    return {
        "dni": row['dni'],
        "ape_paterno": ape_paterno,
        "ape_materno": ape_materno,
        "nombres": row['fullname'],
        "fecha_ingreso": fecha_ingreso,
        "estado_civil": ESTADO_CIVIL["SOLTERO"],  # Enviar solo código, Ese dato vendrá en la fase 4
        "tipo_instruccion": GRADO_INSTRUCCION["SECUNDARIA COMPLETA"], # Ese dato vendrá en la fase 4
        # Consultar si es el mismo para la fecha de nacimiento
        "ubigeo": row['ubigeo'],
        "fecha_nacimiento": fecha_nacimiento,
        "sexo":  'M' if row['sexo'].lower() == 'masculino' else 'F',
        "celular": row['cellphone'],
        "email": row['email'],
        "direccion": direccion,
        "tipo_via": cod_tipo_via,  # Código OFIPLAN
        "tipo_zona": cod_tipo_zona,  # Código OFIPLAN
        "pais": "155",  # Código OFIPLAN (PERU)
        "nacionalidad": "155",  # Código OFIPLAN (PERUANO)
        "cod_pensionario": REGIMEN_PENSIONARIO[regimen_pensionario]
    }


def get_trabajadores_no_existen():
    df_ofiplan = get_trabajadores_ofiplan()
    df_rrhh = get_trabajadores_rrhh()
    # Convertir las columnas de identificación a listas
    trabajadores_ofiplan = df_ofiplan['CO_TRAB'].tolist()
    trabajadores_rrhh = df_rrhh['dni'].tolist()

    # Filtrar trabajadores que están en BD_RRHH pero no en BD_OFIPLAN
    df_trabajadores_no_existen = df_rrhh[~df_rrhh['dni'].isin(
        trabajadores_ofiplan)]

    print("Trabajadores que no existen en BD_OFIPLAN:")
    print(df_trabajadores_no_existen)

    # Aplicar la función a cada fila del DataFrame
    json_data = [formatear_json(row)
                 for _, row in df_trabajadores_no_existen.iterrows()]

    # Convertir la lista de diccionarios a un JSON
    r_json = json.dumps(json_data, indent=2)

    # Imprimir el JSON
    print(r_json)

    return r_json
    # Convertir a JSON
    result_json = df_trabajadores_no_existen.to_json(orient='records')
    print(result_json)

    # Para pruebas en UIPATH envio un ejemplo de lo q se enviara en el json
    # Ejemplo de json a formar para enviar al bot UIPATH
    r_json = [
        {
            "dni": "74060206",
            "fullname": "KEVIN ANTHONY",
            "lastname": "NOLASCO CAMONES",
            "surname": "",
            "idtypeemployee": 1,
            # Dato faltantes
            "fecha_ingreso": "20/10/2024",
            "ape_paterno": "NOLASCO",
            "ape_materno": "CAMONES",
            "nombres": "KEVIN ANTHONY",
            "estado_civil": "SOLTERO",
            "tipo_instruccion": "SECUNDARIA COMPLETA",
            "ubigeo": "020101",
            "fecha_nacimiento": "26/12/1995",
            # pais: TODO ES PERU, y Nacinalidad igula PERU : 155
            "sexo": "M",
            "celular": "999999999",
            "email": "XXXXXXXXXX@gmail.com",
            # CARR. CARRETERA CENTRAL NRO. SAN LUIS - CARLOS FERMIN FITZCA - ANCASH
            "direccion": "AV. LA PAZ 123",
            "tipo_via": "04",  # "PASAJE", # Codigo OFIPLAN ejemplo 04: PASAJE
            "tipo_zona": "12",  # "BARRIO", # Código OFIPLAN  ejemplo 12: BARRIO
            "pais": "155",  # "PERU", # Codigo en OFIPLAN: 155
            "nacionalidad":  "155",  # "PERUANO", # Código OFIPLAN: 155
            "cod_pensionario": "INT",
        },
        # {
        #     "dni": "47518569",
        #     "fullname": "KEVI",
        #     "lastname": "NOLASCO CAMONES",
        #     "surname": "",
        #     "idtypeemployee": 1,
        #     # Dato faltantes
        #     "fecha_ingreso": "19/10/2024",
        #     "ape_paterno": "NOLASCO",
        #     "ape_materno": "CAMONES",
        #     "nombres": "CARLA ANTHONY",
        #     "estado_civil": "SOLTERO",
        #     "tipo_instruccion": "SECUNDARIA COMPLETA",
        #     "ubigeo": "020101",
        #     "fecha_nacimiento": "26/12/1993",
        #     # pais: TODO ES PERU, y Nacinalidad igula PERU : 155
        #     "sexo": "F",
        #     "celular": "999999999",
        #     "email": "YYYYYYYY@gmail.com",
        #     # CARR. CARRETERA CENTRAL NRO. SAN LUIS - CARLOS FERMIN FITZCA - ANCASH
        #     "direccion": "AV. LA PAZ 123",
        #     "tipo_via": "04", # "PASAJE", # Codigo OFIPLAN ejemplo 04: PASAJE
        #     "tipo_zona": "12", # "BARRIO", # Código OFIPLAN  ejemplo 12: BARRIO
        #     "pais": "155", #"PERU", # Codigo en OFIPLAN: 155
        #     "nacionalidad":  "155", # "PERUANO", # Código OFIPLAN: 155
        #     "cod_pensionario": "INT",
        # },
    ]
    # print(r_json)
    return json.dumps(r_json)

# OPCION 2 Intentar pasar una tupla
# def get_trabajadores_no_existen():
#     df_ofiplan = get_trabajadores_ofiplan()
#     df_rrhh = get_trabajadores_rrhh()
#     # Convertir las columnas de identificación a listas
#     trabajadores_ofiplan = df_ofiplan['CO_TRAB'].tolist()
#     trabajadores_rrhh = df_rrhh['dni'].tolist()

#     # Filtrar trabajadores que están en BD_RRHH pero no en BD_OFIPLAN
#     df_trabajadores_no_existen = df_rrhh[~df_rrhh['dni'].isin(trabajadores_ofiplan)]

#     print("Trabajadores que no existen en BD_OFIPLAN:")
#     print(df_trabajadores_no_existen)

#     columns = df_trabajadores_no_existen.columns.tolist()
#     rows = df_trabajadores_no_existen.values.tolist()
#     print(columns, rows)
#     return columns, rows


print(get_trabajadores_no_existen())
