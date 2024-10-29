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

# Crear la conexión SQL
try:
    # Conectar a SQL Server (BD_OFIPLAN)
    conn_sql_server = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};' # Controlador ODBC
        f'SERVER={server_sql};'
        f'DATABASE={database_sql};'
        f'UID={username_sql};'
        f'PWD={password_sql};'
    )
    print("Conexión exitosa a SQL Server")
except Exception as e:
    print("Error al conectar a SQL Server:", e)

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

# Hacer una consulta de prueba (opcional)
# cursor = conexion.cursor()
# cursor.execute("SELECT @@version;")
# row = cursor.fetchone()
# print(f"Versión de SQL Server: {row[0]}")
# conexion.close()

## Consulta para obtener los trabajadores de BD_OFIPLAN
sql_query_ofiplan = """
    SELECT CO_TRAB, NO_APEL_PATE, NO_APEL_MATE, NO_TRAB
    FROM [OFIPLAN].[dbo].[TMTRAB_PERS]
"""
df_ofiplan = pd.read_sql(sql_query_ofiplan, conn_sql_server)
print(df_ofiplan)
conn_sql_server.close()


## Consulta para obtener los trabajadores de BD_RRHH
# idtypeemployee: 1 = PLANILLA
sql_query_rrhh = """
    SELECT
        dni,
        fullname,
        lastname,
        surname,
        e.idtypeemployee
    FROM public.employee e
    WHERE e.idtypeemployee = 1;
"""
df_rrhh = pd.read_sql(sql_query_rrhh, conn_postgres)
print(df_rrhh)
conn_postgres.close()

# Convertir las columnas de identificación a listas
trabajadores_ofiplan = df_ofiplan['CO_TRAB'].tolist()
trabajadores_rrhh = df_rrhh['dni'].tolist()

# Filtrar trabajadores que están en BD_RRHH pero no en BD_OFIPLAN
trabajadores_no_existen = df_rrhh[~df_rrhh['dni'].isin(trabajadores_ofiplan)]

print("Trabajadores que no existen en BD_OFIPLAN:")
print(trabajadores_no_existen)

def get_trabajadores_no_existen():
    # Devolver el resultado como DataFrame
    return trabajadores_no_existen