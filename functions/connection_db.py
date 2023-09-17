import pypyodbc as db
import datetime

from functions.mail import enviar_mail_con_error
from config.config import UID, PWD


def log(mensaje):

    now = datetime.datetime.now()
    fecha = str(now.strftime("%Y_%m_%d %H:%M:%S"))

    print("["+str(fecha)+"]: " + str(mensaje))

def conexion_bd_datos(server, base_datos, consulta, type=None, execute=None):

    try:
        cnxn = db.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=" + server + ";"
            "DATABASE=" + base_datos + ";"
            "UID=" + UID + ";"
            "PWD=" + PWD + ";"
            "TrustServerCertificate=no;"
            "Connection Timeout=60;"
            "autocommit=True"
        )

        if type == "retorna_valor":
            resultado = []
            cursor = cnxn.cursor()
            cursor.execute(consulta)
            for row in cursor:
                resultado.append(row)
            cnxn.close()
            cursor.close()

            return resultado

        elif type == "sin_retorno":
            cursor = cnxn.cursor()
            cursor.executemany(consulta) if execute is None else cursor.execute(consulta)
            cnxn.commit()
            cnxn.close()
            cursor.close()
        else:
            cursor = cnxn.cursor()
            cursor.execute(consulta)

            resultado = cursor.fetchone()

            cnxn.commit()
            cnxn.close()
            cursor.close()

            return resultado[0]
    except Exception as e :
        log(str(e))
        enviar_mail_con_error(str(e))
        
