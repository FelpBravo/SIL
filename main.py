from functions.proceso import preparacion, obtener_archivos, insercion_datos, consulta_datos, comprimir_archivos
from functions.bitacora import insertar_bitacora, actualizar_bitacora
from functions.sftp import traspaso_sfpt
from functions.mail import enviar_mail_con_error
from config.log import logger

estado_proceso=True

try:
    logger("Inicio del proceso para Reporte SIL Masivo")
    id_instancia= insertar_bitacora("10400")

    logger("Ejecutando SPs...")
    preparacion()

    logger("Obteniendo datos desde servidor SFTP.")
    archivos = obtener_archivos()
    print(archivos)
    logger("Creando archivos de respuesta.")
    for ar in archivos:
        insercion_datos(ar)
        datos_archivo = consulta_datos(ar)
        archivo_final = comprimir_archivos(datos_archivo[0][0], datos_archivo[0][1], datos_archivo[1])
        traspaso_sfpt(archivo_final[0],archivo_final[1])

    logger("Proceso termina OK.")
except Exception as e:
    estado_proceso=False
    logger("Proceso termina NOK.")
    enviar_mail_con_error(str(e))
    
finally:
    actualizar_bitacora(id_instancia, id_proceso="10400", estado=estado_proceso)