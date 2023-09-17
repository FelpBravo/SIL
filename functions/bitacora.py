from functions.mail import enviar_mail_con_error
from config.config import SERVER_INTEGRA, BD_INTEGRA
from config.log import logger
from functions.connection_db import conexion_bd_datos


spUPD_Control_Procesos_Gestion = '''exec [dbo].[**********] {id_instancia},{estado},'{resultado}',{cant_total},{cant_ok},{cant_err},NULL'''
spINS_Control_Procesos_Gestion = "DECLARE @RC bigint; exec [dbo].[**********] {id_proceso},{bloque_proceso},@RC output,NULL; SELECT @RC AS rc;"


def insertar_bitacora(id_proceso, bloque_proceso="0"):
    try:
        id_instancia = conexion_bd_datos(
            SERVER_INTEGRA,
            BD_INTEGRA,
            spINS_Control_Procesos_Gestion.format(id_proceso=id_proceso, bloque_proceso=bloque_proceso)
        )
        return id_instancia
    except Exception as e:
        logger(str(e))
        enviar_mail_con_error(str(e))


def actualizar_bitacora(id_instancia, id_proceso="", nombre_funcion="", error="", xml="", estado=True, cant_total='NULL', cant_ok='NULL', cant_err='NULL'):
    try:
        if id_proceso == "10400":
            xml = "<Result>\n"
            xml += "<InformaciónProceso>\n"
            xml += "<NombreProceso>SIL Reporte Masivo</NombreProceso>\n"
            xml += "<Version>1</Version>\n"
            xml += "</InformaciónProceso>\n"
            xml += "</Result>\n"
        else:
            xml = "<Result>\n"
            xml += "<InformaciónProceso>\n"
            xml += "<NombreProceso>SIL Reporte Masivo</NombreProceso>\n"
            xml += f'<NombreFuncion>{nombre_funcion}</NombreFuncion>\n'
            xml += f'<Estado> {"OK" if estado else "NOK"} </Estado>\n'
            if error != "":
                xml += f'<DescripcionExcepcion> {error} </DescripcionExcepcion>\n'
            xml += "</InformaciónProceso>\n"
            xml += "</Result>\n"

        conexion_bd_datos(
            SERVER_INTEGRA,
            BD_INTEGRA,
            spUPD_Control_Procesos_Gestion.format(
                id_instancia=str(id_instancia),
                estado="1" if estado else "0",
                resultado=str(xml).replace("'", '''"'''),
                cant_total=str(cant_total),
                cant_ok=str(cant_ok),
                cant_err=str(cant_err)
            ),
            "sin_retorno", "Si"
        )
    except Exception as e:
        logger(str(e))
        enviar_mail_con_error(str(e))
