import pysftp
import warnings

from functions.mail import enviar_mail_con_error
from functions.bitacora import insertar_bitacora, actualizar_bitacora
from config.log import logger
from config.config import username, password, host, REMOTE_HOME_PATH


warnings.simplefilter(action='ignore', category=UserWarning)


def traspaso_sfpt(afp, archivo):
    logger(
        f'Traspasando archivo comprimido al servidor remoto dentro del directorio {REMOTE_HOME_PATH+ "0"+str(afp)+"/Salida"}')
    estado = True
    id_instancia = insertar_bitacora("10406")
    informacion_error = ""
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    try:
        with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
            logger("Conexion establecida satisfactoriamente.")
            sftp.cwd(REMOTE_HOME_PATH + "0"+str(afp)+"/Salida")
            logger("Traspasando archivo " + archivo)
            sftp.put(archivo)
        sftp.close()

    except Exception as er:
        estado = False
        informacion_error = str(er)
        logger(str(er))
        enviar_mail_con_error(str(er))

    actualizar_bitacora(id_instancia=id_instancia, estado=estado,
                        nombre_funcion="traspaso_sfpt", error=informacion_error)
