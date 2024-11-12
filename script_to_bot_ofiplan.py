import json
import pyodbc
import psycopg2
import pandas as pd
import requests
from datetime import datetime


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
    'SIN REG. PENSIONARIO': '99',
    '': '99'
}

# Departamentos
DEPARTAMENTOS = {
    'AREA DE ADMINISTRACION': 'MT110',  # 'DEPARTAMENTO ADMINISTRACION'
    'DEPARTAMENTO DE ALMACEN': 'MT1100',
    'AREA DE CONTABILIDAD': 'MT350',  # 'DEPARTAMENTO DE CONTABILIDAD'
    'AREA DE LOGISTICA': 'MT200',  # 'DEPARTAMENTO DE LOGISTICA'
    'AREA DE RRHH': 'MT300',  # 'DEPARTAMENTO DE RECURSOS HUMANOS'
    'AREA DE TECNOLOGIA': 'MT250',  # 'DEPARTAMENTO DE TECNOLOGIA DE LA INFORMACION'
    'AREA DE TESORERIA': 'MT400',  # 'DEPARTAMENTO DE TESORERIA'
    'DEPARTAMENTO DE TRANSPORTES': 'MT450',
    'DEPARTAMENTO GERENCIA': 'MT100',
    'AREA LEGAL': 'MT140',  # 'DEPARTAMENTO LEGAL'
    'MARKET BOLIVAR': 'MT1050',
    'MARKET CARAZ': 'MT850',
    'MARKET CARHUAZ': 'MT800',
    'MARKET JR. CARAZ': 'MT650',
    'MARKET LUZURIAGA': 'MT700',
    'MARKET RAYMONDI': 'MT1150',
    'MARKET SUCRE': 'MT750',
    'MAYORISTA CARAZ': 'MT550',
    'MAYORISTA CARHUAZ': 'MT600',
    'MAYORISTA RAYMONDI': 'MT500',
    'POR DEFINIR DEPARTAMENTE': '999',
    'PRODUCCION CENTER': 'MT900',
    'PROYECTOS DE INGENIERIA': 'MT950',
    'SUPERVISION DE CONSTRUCCIONES': 'MT150',
    'TRUJILLO CENTER': 'MT1000'
}

# sedes
SEDES = {
    'AREA DE ADMINISTRACION': '010',  # 'JR.SIMON BOLIVAR NRO 933'
    'DEPARTAMENTO DE ALMACEN': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE CONTABILIDAD': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE LOGISTICA': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE RRHH': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE TECNOLOGIA': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE TESORERIA': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'DEPARTAMENTO DE TRANSPORTES': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'DEPARTAMENTO GERENCIA': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'AREA LEGAL': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'MARKET BOLIVAR': '010',  # 'JR.SIMON BOLIVAR NRO 933',
    'MARKET CARAZ': '001',  # Av. 1ERO DE MAYO
    'MARKET CARHUAZ': '006',  # 'JR. UCAYALI',
    'MARKET JR. CARAZ': '005',  # 'JR. CARAZ NRO. 344',
    'MARKET LUZURIAGA': '002',  # 'AV. LUZURIAGA NRO. 586',
    'MARKET RAYMONDI': '012',  # PROLOGANCION ANTONIO RAYMONDI
    'MARKET SUCRE': '004',  # 'JR. SUCRE',
    'MAYORISTA CARAZ': '008',  # 'CAR. CENTRAL NRO. 632',
    'MAYORISTA CARHUAZ': '007',  # 'AV PROGRESO',
    'MAYORISTA RAYMONDI': '003',  # AV RAYMONDI
    'PRODUCCION CENTER': '009',  # 'CARRETERA HUARAZ - MASHUAN'
    'PROYECTOS DE INGENIERIA': '010',
    'SUPERVISION DE CONSTRUCCIONES': '010',
    'TRUJILLO CENTER': '009',  # 'CARRETERA HUARAZ - MASHUAN'
}

"""
SEDES = {
    'AREA DE ADMINISTRACION': 'JR.SIMON BOLIVAR NRO 933', #Codigo OFIPLAN: 010
    'DEPARTAMENTO DE ALMACEN': 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE CONTABILIDAD': 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE LOGISTICA': 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE RRHH': 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE TECNOLOGIA': 'JR.SIMON BOLIVAR NRO 933',
    'AREA DE TESORERIA': 'JR.SIMON BOLIVAR NRO 933',
    'DEPARTAMENTO DE TRANSPORTES': 'JR.SIMON BOLIVAR NRO 933',
    'DEPARTAMENTO GERENCIA': 'JR.SIMON BOLIVAR NRO 933',
    'AREA LEGAL': 'JR.SIMON BOLIVAR NRO 933',
    'MARKET BOLIVAR': 'JR.SIMON BOLIVAR NRO 933',
    'MARKET CARAZ' : 'CAR. CENTRAL NRO. 632', # ??
    'MARKET CARHUAZ' : 'JR. UCAYALI',
    'MARKET JR. CARAZ' : 'JR. CARAZ NRO. 344',
    'MARKET LUZURIAGA' : 'AV. LUZURIAGA NRO. 586',
    'MARKET RAYMONDI' : 'AV. RAYMONDI NRO. 351',
    'MARKET SUCRE' : 'JR. SUCRE',
    'MAYORISTA CARAZ' : '',
    'MAYORISTA CARHUAZ' : '',
    'MAYORISTA RAYMONDI' : '',
    'PRODUCCION CENTER': '',
    'PROYECTOS DE INGENIERIA' : '',
    'SUPERVISION DE CONSTRUCCIONES': '',
    'TRUJILLO CENTER': 'CARRETERA HUARAZ - MASHUAN'
}
"""


# Configuración de las conexiones
DB_CONFIG = {
    'sql_server': {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': '10.5.0.9',
        'database': 'OFIPLAN',
        'username': 'admin',
        'password': 'Huaraz2022..*'
    },
    'postgresql': {
        'server': '40.86.9.189',
        'database': 'RhDB2',
        'username': 'sqladmin',
        'password': 'Slayer20fer..',
        'port': '5433'
    },
    'postgresql_dev': {
        'server': 'localhost',
        'database': 'RhDB2',
        'username': 'postgres',
        'password': '1234',
        'port': '5432'
    }
}


# Función para conectar a SQL Server
def connect_sql_server():
    try:
        conn = pyodbc.connect(
            f"DRIVER={DB_CONFIG['sql_server']['driver']};"
            f"SERVER={DB_CONFIG['sql_server']['server']};"
            f"DATABASE={DB_CONFIG['sql_server']['database']};"
            f"UID={DB_CONFIG['sql_server']['username']};"
            f"PWD={DB_CONFIG['sql_server']['password']}"
        )
        return conn
    except Exception as e:
        print("Error al conectar a SQL Server:", e)
        return None


# Función para conectar a PostgreSQL
def connect_postgresql():
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['postgresql']['database'],
            user=DB_CONFIG['postgresql']['username'],
            password=DB_CONFIG['postgresql']['password'],
            host=DB_CONFIG['postgresql']['server'],
            port=DB_CONFIG['postgresql']['port']
        )
        return conn
    except Exception as e:
        print("Error al conectar a PostgreSQL:", e)
        return None


def get_trabajadores_ofiplan():
    query = "SELECT CO_TRAB, NO_APEL_PATE, NO_APEL_MATE, NO_TRAB FROM [OFIPLAN].[dbo].[TMTRAB_PERS]"
    conn = connect_sql_server()
    if conn:
        df = pd.read_sql(query, conn)
        conn.close()
        # print("Trabajadores desde OFIPLAN")
        # print(df)
        return df
    return pd.DataFrame()

###
    # 0 #75891884 OK <- Ya esta en maestro empresa
    # 1 #75139521 OK
    # 2 #75912867 OK
    # 3 #60411189 OK
    # 4 #60730169 OK
    # 5 #75230396 OK
    # 6 #48140024 OK
    # 7 #71207435 OK
    # 8 #71511047 OK
    # 9 #75785131 OK
    # 10 #76172731 OK
    # 11 #60027120 OK
    ####


def get_trabajadores_rrhh_old(dni_list=None):
    # deprecated by API rrhh
    """
    Obtiene los datos de trabajadores desde la base de datos BD_RRHH en PostgreSQL.
    Si se proporciona una lista de DNIs, solo devuelve los trabajadores con esos DNIs.
    """

    # Construir la consulta SQL
    sql_query_rrhh = """
        SELECT
            emp.dni,
            emp.fullname,
            emp.lastname,
            emp.birthdate,
            emp.cellphone,
            emp.email,
            emp.address,
            emp.idsite,
            b.name AS departamento,
            emp.idcharge,
            emp.startdate,
            emp.salary,
            emp.sexo,
            emp.condition,
            emp.licence,
            emp.id_afp,
            a.nombre AS regimen_pensionario,
            emp.id_entidadfinanciera,
            emp.cci,
            emp.cargo,
            emp.ubigeo
            -- emp.cci_cts
        FROM
            employee emp
        LEFT JOIN
            business b ON b.id = emp.idsite
        LEFT JOIN
            afp a ON a.id = emp.id_afp
        WHERE
            emp.idtypeemployee = 1
            AND emp.status = true
    """

    # Agregar el filtro de DNI si se proporciona una lista
    if dni_list:
        # Convertir la lista de DNIs a una cadena compatible con SQL
        # Convertir la lista a una tupla para el formato SQL
        dni_tuple = tuple(dni_list)
        sql_query_rrhh += f" AND emp.dni IN {dni_tuple}"

    conn = connect_postgresql()
    if conn:
        df = pd.read_sql(sql_query_rrhh, conn)
        conn.close()
        print("Trabajadores desde la BD RRHHH")
        print(df)
        return df
    return pd.DataFrame()  # Devolver un DataFrame vacío en caso de error


def get_trabajadores_rrhh(dni_list=None):
    """
    Obtiene los datos de trabajadores desde la API en lugar de la base de datos.
    Si se proporciona una lista de DNIs, solo devuelve los trabajadores con esos DNIs.
    """
    api_url = "https://trv-app-rrhh-dtdmgvb6asf8afgd.westus-01.azurewebsites.net//offiplan/trabajdores"

    try:
        # Realizar la solicitud GET a la API
        response = requests.get(api_url)
        response.raise_for_status()  # Verificar si la respuesta fue exitosa (código 200)
        data = response.json().get("data", [])

        # Convertir la respuesta JSON en un DataFrame
        df = pd.DataFrame(data)

        # Filtrar por lista de DNIs si se proporciona
        if dni_list:
            df = df[df['dni'].isin(dni_list)]

        print("Datos obtenidos desde la API RRHH")
        print(df)
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener datos desde la API: {e}")
        return pd.DataFrame()  # Devolver un DataFrame vacío en caso de error

# print(get_trabajadores_rrhh())


def normalizar_direccion(direccion, abreviaturas):
    """Función para reemplazar abreviaturas con su equivalente completo"""
    for abrev, completo in abreviaturas.items():
        direccion = direccion.upper().replace(abrev, completo)
    return direccion


def obtener_codigo_via_y_zona(direccion):
    """Función para obtener los códigos"""
    direccion_normalizada_via = normalizar_direccion(
        direccion, ABREVIATURAS_VIAS)
    direccion_normalizada_zona = normalizar_direccion(
        direccion, ABREVIATURAS_ZONAS)

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


def get_ubigeo_to_ofiplan(ubigeo):
    if "0206" in ubigeo:
        # 0206 -> CARHUAZ
        return "020401"
    if "0212" in ubigeo:
        # 0212 -> CARAZ
        return "020701"
    if "0201" in ubigeo:
        # 0201 -> HUARAZ
        return "020101"
    return ubigeo


def get_jefe_inmediato_old(departamento):
    # deprecated by API rrhh
    '''Obtener el DNI del jefe inmediato según el departamento'''
    # Cargos asignados según el departamento
    cargo_jefe_by_departamento = {
        'MARKET': 'Administrador de Tienda',
        'MAYORISTA': 'Jefe de Tienda Mayorista',
        'AREA DE TECNOLOGIA': 'Data Engineer',
        'AREA DE CONTABILIDAD': 'Head of Accounting and Treasury',
        'TRUJILLO CENTER': 'Jefe de Tienda Mayorista',
        'AREA LOGISTICA': 'Purchasing Manager'
    }

    _departamento = departamento
    # Identificar departamento general
    if 'MARKET' in departamento.upper():
        _departamento = 'MARKET'

    if 'MAYORISTA' in departamento.upper():
        _departamento = 'MAYORISTA'

    # Verificar que el departamento tenga un cargo asignado
    if _departamento not in cargo_jefe_by_departamento:
        print("Departamento no encontrado")
        return None

    # Obtener el cargo correspondiente
    cargo = cargo_jefe_by_departamento[_departamento]

    # Construir la consulta SQL dinámica para obtener solo el DNI
    sql_query_rrhh = f"""
        SELECT
            emp.dni
        FROM
            employee emp
        LEFT JOIN
            business b ON b.id = emp.idsite
        WHERE
            emp.idtypeemployee = 1
            AND emp.status = true
            AND b.name = '{departamento}'
            AND emp.cargo = '{cargo}'
        LIMIT 1
    """

    # Crear y ejecutar la conexión a PostgreSQL
    try:
        conn = connect_postgresql()
        if conn:
            df = pd.read_sql(sql_query_rrhh, conn)
            conn.close()
            # Si hay resultados, devolver el primer DNI; si no, devolver None
            return df['dni'].iloc[0] if not df.empty else None
        return pd.DataFrame()

    except Exception as e:
        print("Error en la consulta:", e)
        return None


def get_jefe_inmediato(departamento):
    """
    Obtener el DNI del jefe inmediato según el departamento usando una API.
    """
    # Cargos asignados según el departamento
    cargoJefeByDepartamento = {
        'MARKET': 'Administrador de Tienda',
        'MAYORISTA': 'Jefe de Tienda Mayorista',
        'AREA DE TECNOLOGIA': 'Data Engineer',
        'AREA DE CONTABILIDAD': 'Head of Accounting and Treasury',
        'TRUJILLO CENTER': 'Jefe de Tienda Mayorista',
        'AREA LOGISTICA': 'Purchasing Manager'
    }

    # Normalizar el nombre del departamento para determinar el cargo
    _departamento = departamento
    if 'MARKET' in departamento.upper():
        _departamento = 'MARKET'
    elif 'MAYORISTA' in departamento.upper():
        _departamento = 'MAYORISTA'

    # Verificar que el departamento tenga un cargo asignado
    if _departamento not in cargoJefeByDepartamento:
        print("Departamento no encontrado")
        return None

    # Obtener el cargo correspondiente
    cargo = cargoJefeByDepartamento[_departamento]

    # Construir la URL de la API con los parámetros
    api_url = f"https://trv-app-rrhh-dtdmgvb6asf8afgd.westus-01.azurewebsites.net/offiplan/jefeInmediato/{departamento}/{cargo}"

    try:
        # Realizar la solicitud GET a la API
        response = requests.get(api_url)
        response.raise_for_status()  # Verificar si la respuesta fue exitosa (código 200)

        # Extraer el JSON de la respuesta
        data = response.json()

        # Verificar si la respuesta contiene datos y devolver el DNI
        if data.get("success") and data.get("data"):
            dni = data["data"][0].get("dni")
            return dni if dni else None
        else:
            print("No se encontró el DNI en la respuesta")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el jefe inmediato desde la API: {e}")
        return None


def cargar_puestos_desde_excel():
    """Carga los datos de puestos y grupos ocupacionales desde un archivo Excel. Ubicado en el mismo lugar de este Script"""
    try:
        file_path = 'C:\script_to_bot_OFIPLAN\PUESTOS_GRUPOS.xlsx'
        # Leer el archivo Excel y forzar 'COD_GRUPO' como tipo string
        df = pd.read_excel(file_path, dtype={'COD_GRUPO': str})

        # Convertir todos los valores a mayúsculas para estandarizar
        df['PUESTO'] = df['PUESTO'].str.upper()
        df['GRUPO OCUPACIONAL'] = df['GRUPO OCUPACIONAL'].str.upper()
        df['COD_GRUPO'] = df['COD_GRUPO'].str.upper()

        # Reemplazar valores 'NaN' en 'COD_GRUPO' y 'GRUPO OCUPACIONAL' con 'NO APLICA'
        df['COD_GRUPO'].fillna('NA', inplace=True)
        df['GRUPO OCUPACIONAL'].fillna('NO APLICA', inplace=True)

        return df
    except Exception as e:
        print(f"Error al cargar el archivo Excel: {e}")
        return pd.DataFrame()  # Devuelve un DataFrame vacío si hay un error


def get_puesto_and_grupo_to_ofiplan(puesto):
    """Obtiene el grupo ocupacional basado en el puesto desde el DataFrame de puestos."""

    df_puestos = cargar_puestos_desde_excel()

    # Filtrar el DataFrame por el puesto especificado
    filtro = df_puestos[df_puestos['PUESTO'] == puesto.upper()]

    # Si el puesto existe en el DataFrame, obtener sus valores, si no, asignar 'NO APLICA'
    if not filtro.empty:
        cod_grupo = filtro['COD_GRUPO'].values[0]
        grupo_ocupacional = filtro['GRUPO OCUPACIONAL'].values[0]
    else:
        cod_grupo = "NA"
        grupo_ocupacional = "NO APLICA"

    return cod_grupo, grupo_ocupacional


# print(get_puesto_and_grupo_to_ofiplan('junior Accounting Specialist - advances'))

def convertir_fecha(fecha_str):
    """Convierte una fecha en formato string a formato '%d/%m/%Y'."""
    if fecha_str:
        try:
            # Intentar convertir la fecha desde el formato proporcionado por la API
            fecha_dt = datetime.strptime(fecha_str, '%a, %d %b %Y %H:%M:%S %Z')
            return fecha_dt.strftime('%d/%m/%Y')
        except ValueError:
            print(f"Error al convertir la fecha: {fecha_str}")
    return ""


def formatear_json(row):
    """Función para agregar los campos adicionales y formar el JSON"""
    print(row)
    # Dividir el apellido en paterno y materno
    apellidos = row['lastname'].split(" ", 1)
    ape_paterno = apellidos[0]
    ape_materno = apellidos[1] if len(apellidos) > 1 else ""

    # # # Formatear las fechas
    fecha_ingreso = convertir_fecha(row.get('startdate'))
    fecha_nacimiento = convertir_fecha(row.get('birthdate'))

    direccion = row['address']
    cod_tipo_via, cod_tipo_zona = obtener_codigo_via_y_zona(direccion)
    regimen_pensionario = row["regimen_pensionario"].replace(" ", "").split("-")[0] if row.get("regimen_pensionario") else ""
    departamento = row.get("departamento", "")
    puesto = row.get("cargo", "")
    cod_grupo, _ = get_puesto_and_grupo_to_ofiplan(puesto)

    # Crear el JSON con los datos requeridos
    return {
        "dni": row['dni'],
        "ape_paterno": ape_paterno,
        "ape_materno": ape_materno,
        "nombres": row['fullname'],
        "fecha_ingreso": fecha_ingreso,
        # Enviar solo código, Ese dato vendrá en la fase 4 ???
        "estado_civil": ESTADO_CIVIL["SOLTERO"],
        # Ese dato vendrá en la fase 4 ???
        "tipo_instruccion":  GRADO_INSTRUCCION["SECUNDARIA COMPLETA"] if "MARKET" in departamento or "MAYOR" in departamento else GRADO_INSTRUCCION["SUPERIOR COMPLETA"],
        # # # Consultar si es el mismo para la fecha de nacimiento
        "ubigeo": get_ubigeo_to_ofiplan(row['ubigeo']),
        "fecha_nacimiento": fecha_nacimiento,
        "sexo":  'M' if row['sexo'].lower() == 'masculino' else 'F',
        "celular": row['cellphone'],
        "email": row['email'],
        "direccion": direccion,
        "tipo_via": cod_tipo_via,  # Código OFIPLAN
        "tipo_zona": cod_tipo_zona,  # Código OFIPLAN
        "pais": "155",  # Código OFIPLAN (PERU)
        "nacionalidad": "155",  # Código OFIPLAN (PERUANO)
        "regimen_pensionario": regimen_pensionario,
        "cod_pensionario": REGIMEN_PENSIONARIO.get(regimen_pensionario, ""),
        # # # DATOS PARA MODULO MAESTRO EMPRESA
        "departamento_desc": departamento,
        "departamento": DEPARTAMENTOS.get(departamento, ""),
        "sede": SEDES.get(departamento, ""),  # Dirección de la sede
        "puesto": puesto,  # 'almacenero', # SE enviara la descripción
        "grupo_ocupacional": cod_grupo,  # Poner Código
        "banco_trabajador": "CRE" if departamento in ["MAYORISTA CARAZ", "MARKET CARAZ"] else "CON",
        # Campo en la Fase 4 RRHH
        "nro_cuenta_cts": row.get("cci_cts", "0000000000000"),
        "turno": "002" if "MARKET" in departamento or "MAYOR" in departamento or "CENTER" in departamento else "001",  # 002: MARKETS Y MAY
        "jefe": get_jefe_inmediato(departamento),  # DNI
    }


def get_trabajadores_no_existen():
    df_ofiplan = get_trabajadores_ofiplan()
    df_rrhh = get_trabajadores_rrhh()
    # # Convertir las columnas de identificación a listas
    trabajadores_ofiplan = df_ofiplan['CO_TRAB'].tolist()

    # # Filtrar trabajadores que están en BD_RRHH pero no en BD_OFIPLAN
    df_trabajadores_no_existen = df_rrhh[~df_rrhh['dni'].isin(trabajadores_ofiplan)]

    # # PARA PRUEBAS
    # # df_trabajadores_no_existen = df_rrhh
    # print("Trabajadores que no existen en BD_OFIPLAN:")
    # print(df_trabajadores_no_existen)

    # # Aplicar la función a cada fila del DataFrame
    json_data = [formatear_json(row) for _, row in df_trabajadores_no_existen.iterrows()]

    # Convertir la lista de diccionarios a un JSON
    r_json = json.dumps(json_data, indent=2)

    # Imprimir el JSON
    print(r_json)

    return r_json


def get_trabajadores_ofiplan_maestro_empresa():
    """Consulta para obtener Trabajadores que pertenecen a Maestro empresa OFIPLAN"""
    sql_query_ofiplan = """
        SELECT [CO_TRAB]
        FROM [OFIPLAN].[dbo].[TMTRAB_EMPR]
    """

    conn = connect_sql_server()
    if conn:
        df = pd.read_sql(sql_query_ofiplan, conn)
        conn.close()
        print("Trabajadores que pertenecen a Maestro empresa OFIPLAN")
        print(df)
        return df
    return pd.DataFrame()


def get_trabajadores_no_existen_modulo_maestro_empresa():
    """
    Obtiene la lista de trabajadores que existen en el módulo maestro personal pero que no existen en el módulo maestro empresa en OFIPLAN.
    """
    # Obtener los DataFrames de los dos módulos en OFIPLAN
    df_ofiplan = get_trabajadores_ofiplan()
    df_ofiplan_maestro_empresa = get_trabajadores_ofiplan_maestro_empresa()

    # Validar que las columnas necesarias existen en ambos DataFrames
    if 'CO_TRAB' not in df_ofiplan.columns or 'CO_TRAB' not in df_ofiplan_maestro_empresa.columns:
        print("Error: La columna 'CO_TRAB' no existe en uno de los DataFrames.")
        return None

    # Filtrar trabajadores que están en el módulo maestro personal pero no en el módulo maestro empresa
    df_trabajadores_no_existen = df_ofiplan[~df_ofiplan['CO_TRAB'].isin(
        df_ofiplan_maestro_empresa['CO_TRAB'])]

    if df_trabajadores_no_existen.empty:
        print("Todos los trabajadores en el módulo maestro personal están en el módulo maestro empresa.")
        return json.dumps([])

    # Mostrar resultados
    # print("Trabajadores que no existen en el módulo maestro empresa:")
    # print(df_trabajadores_no_existen)

    dni_list = df_trabajadores_no_existen['CO_TRAB'].tolist()
    df_rrhh = get_trabajadores_rrhh(dni_list)

    print("Trabajadores formateados con BD RRHH que no existen en el módulo maestro empresa:")
    print(df_rrhh)

    # Aplicar la función a cada fila del DataFrame
    json_data = [formatear_json(row)
                 for _, row in df_rrhh.iterrows()]

    # Convertir la lista de diccionarios a un JSON
    r_json = json.dumps(json_data, indent=2)

    # Imprimir el JSON
    print(r_json)

    return r_json


# get_trabajadores_no_existen()
get_trabajadores_no_existen_modulo_maestro_empresa()
# print(get_jefe_inmediato("MARKET JR. CARAZ"))
