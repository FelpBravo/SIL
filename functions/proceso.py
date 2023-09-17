#!/usr/bin/python3.9
from functions.mail import enviar_mail_con_error
from functions.bitacora import insertar_bitacora, actualizar_bitacora
from functions.connection_db import conexion_bd_datos
from config.log import logger
from config.config import BD_GESTION_BI, QA, SERVER_INTEGRA, username, password, host, SERVER_AON, BD_INFO_TEMPORAL, REMOTE_HOME_PATH, hostype

import pysftp
import datetime
import pandas as pd
import zipfile
import os
import warnings

warnings.simplefilter(action='ignore', category=UserWarning)
now = datetime.datetime.now()
fecha_char_8 = '20230208' if QA else str(now.strftime("%Y%m%d"))

### SP y Script SQL ###
nombre_sqlINS_SIL_Consulta_Sil_Masivo = '*****************.sql'
sqlINS_SIL_Consulta_Sil_Masivo = str(
    open(os.getcwd() + "/tools/SQL/" + nombre_sqlINS_SIL_Consulta_Sil_Masivo, "r", encoding="utf8").read())

spINS_SIL_Consulta_Sil_Masivo_gestionbi = "exec [dbo].[*****************]"

nombre_sqlUPD_SIL_Consulta_Sil_Masivo_infotemporal = '*****************.sql'
sqlUPD_SIL_Consulta_Sil_Masivo_infotemporal = str(open(
    os.getcwd() + "/tools/SQL/" + nombre_sqlUPD_SIL_Consulta_Sil_Masivo_infotemporal, "r", encoding="utf8").read())

spUPD_SIL_Consulta_Sil_Masivo_gestionbi = "exec [dbo].[*****************]"
select_in_consulta_sil_masivo = "SELECT * FROM [dbo].[*****************] WITH(NOLOCK)"
spLST_Sil_Detalle_Pagos_Subsidios = "exec [dbo].[*****************] {cod_afp}"
spLST_Sil_Consulta_Sil_Masivo = "exec [dbo].[*****************]"


def preparacion():
    estado = True
    informacion_error = ""
    id_instancia = insertar_bitacora("10401")
    try:
        logger("Ejecutando Script SQL - " + nombre_sqlINS_SIL_Consulta_Sil_Masivo)
        conexion_bd_datos(SERVER_AON, BD_INFO_TEMPORAL, sqlINS_SIL_Consulta_Sil_Masivo, "sin_retorno", "Si")

        logger("Ejecutando SP - [" + BD_GESTION_BI + "] " + spINS_SIL_Consulta_Sil_Masivo_gestionbi)
        conexion_bd_datos(SERVER_INTEGRA, BD_GESTION_BI,
                          spINS_SIL_Consulta_Sil_Masivo_gestionbi, "sin_retorno", "Si")

    except Exception as er:
        estado = False
        informacion_error = str(er)
        logger(str(er))
        enviar_mail_con_error(str(er))

    actualizar_bitacora(id_instancia=id_instancia, estado=estado,
                        nombre_funcion="preparacion", error=informacion_error)


def obtener_archivos():
    folders = ['01003', '01005', '01008', '01032', '01033', '01034', '01035']
    archivos = []
    estado = True
    id_instancia = insertar_bitacora("10402")
    informacion_error = ""
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    try:
        with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
            logger("Conexion establecida satisfactoriamente.")
            for folder in folders:
                for arc in sftp.listdir(f'{REMOTE_HOME_PATH}{folder}/Entrada'):
                    sep_ar = arc.split("_")
                    fecha_archivo = sep_ar[2][:8]
                    if str(fecha_char_8) == fecha_archivo:
                        logger(f'Descargando archivo {arc} Dentro del directorio reportes/Entradas')
                        sftp.get(f'{REMOTE_HOME_PATH}{folder}/Entrada/'+str(arc),
                                 os.getcwd() + "/reportes/Entradas/"+str(arc))
                        archivos.append(os.getcwd() + "/reportes/Entradas/" + str(arc))
        sftp.close()
        return archivos
    except Exception as er:
        estado = False
        informacion_error = str(er)
        logger(str(er))
        enviar_mail_con_error(str(er))

    finally:
        actualizar_bitacora(id_instancia=id_instancia, estado=estado,
                            nombre_funcion="obtener_archivos", error=informacion_error)


def insercion_datos(archivo):
    estado = True
    id_instancia = insertar_bitacora("10403")
    informacion_error = ""
    logger(f'Iniciando inserción de datos en [*********].[dbo].[***********] desde {archivo}')
    try:
        datos = pd.read_csv(archivo, sep=";")
        df = pd.DataFrame(datos)
        cant_lineas = len(df)
        conta_insert = 0
        insert = "insert into [*********].[dbo].[**********] (rut, dv) values"

        for i in df.index:

            if cant_lineas >= 1000:

                if conta_insert == 999:

                    insert += "({0},'{1}');".format(
                        str(df["RUT"][i]), str(df["DV"][i]))
                    cant_lineas = cant_lineas-1000
                    conexion_bd_datos(server=SERVER_AON, base_datos=BD_INFO_TEMPORAL,
                                      type="sin_retorno", consulta=insert)
                    insert = "insert into [*********].[dbo].[**********] (rut, dv) values"
                    conta_insert = 0
                else:
                    insert += "({0},'{1}'),".format(
                        str(df["RUT"][i]), str(df["DV"][i]))
                    conta_insert += 1
            else:

                insert = "insert into [*********].[dbo].[**********] (rut, dv) values({0},'{1}');".format(
                    str(df["RUT"][i]), str(df["DV"][i]))
                conexion_bd_datos(server=SERVER_AON, base_datos=BD_INFO_TEMPORAL,
                                  type="sin_retorno", consulta=insert)

    except Exception as er:
        estado = False
        informacion_error = str(er)
        logger(str(er))
        enviar_mail_con_error(str(er))

    actualizar_bitacora(id_instancia=id_instancia, estado=estado,
                        nombre_funcion="insercion_datos", error=informacion_error)


def consulta_datos(archivo):
    estado = True
    informacion_error = ""
    id_instancia = insertar_bitacora("10404")
    logger(f'Consultando datos sobre el archivo {archivo}')
    try:
        arch_split = archivo.split("_")
        afp = str(arch_split[2 if hostype != 3 else 4])
        fecha_archivo = str(arch_split[3 if hostype != 3 else 5])

        logger("Ejecutando SP - [" + BD_INFO_TEMPORAL + "] " +
               nombre_sqlUPD_SIL_Consulta_Sil_Masivo_infotemporal)
        conexion_bd_datos(SERVER_AON, BD_INFO_TEMPORAL, sqlUPD_SIL_Consulta_Sil_Masivo_infotemporal.format(
            cod_afp=str(afp)), "sin_retorno", "Si")

        logger("Ejecutando Consulta - [" + BD_INFO_TEMPORAL + "] " + select_in_consulta_sil_masivo)
        tabla_con_errores = conexion_bd_datos(
            SERVER_AON, BD_INFO_TEMPORAL, select_in_consulta_sil_masivo, "retorna_valor")

        cant_lineas = len(tabla_con_errores)
        conta_insert = 0
        insert = "insert into [*********].[dbo].[**********] (rut, dv, glosa_retorno) values"

        for i in tabla_con_errores:

            rut = str(i[0])
            dv = str(i[1])
            glosa_retorno = str(i[2])

            if glosa_retorno == 'None':
                glosa_retorno = ""

            if cant_lineas >= 1000:

                if conta_insert == 999:

                    insert += "({0},'{1}','{2}');".format(rut, dv, glosa_retorno)
                    cant_lineas = cant_lineas-1000
                    conexion_bd_datos(server=SERVER_INTEGRA, base_datos=BD_GESTION_BI,
                                      type="sin_retorno", consulta=insert)
                    insert = "insert into [*********].[dbo].[**********] (rut, dv, glosa_retorno) values"
                    conta_insert = 0
                else:
                    insert += "({0},'{1}','{2}'),".format(rut, dv, glosa_retorno)
                    conta_insert += 1
            else:

                insert = "insert into [*********].[dbo].[**********] (rut, dv, glosa_retorno) values({0},'{1}','{2}');".format(
                    rut, dv, glosa_retorno)
                conexion_bd_datos(server=SERVER_INTEGRA, base_datos=BD_GESTION_BI,
                                  type="sin_retorno", consulta=insert)

        logger("Ejecutando SP - [" + BD_GESTION_BI + "]  " + spUPD_SIL_Consulta_Sil_Masivo_gestionbi)
        conexion_bd_datos(SERVER_INTEGRA, BD_GESTION_BI,
                          spUPD_SIL_Consulta_Sil_Masivo_gestionbi, "sin_retorno", "Si")
        salida_final = conexion_bd_datos(
            SERVER_INTEGRA, BD_GESTION_BI, spLST_Sil_Detalle_Pagos_Subsidios.format(cod_afp=str(afp)), "retorna_valor")
        salida_errores = conexion_bd_datos(SERVER_INTEGRA, BD_GESTION_BI,
                                           spLST_Sil_Consulta_Sil_Masivo, "retorna_valor")

        dir_res = os.getcwd() + "/reportes/Salidas/" + str(afp) + "/RESSIL_" + afp+"_"+str(fecha_archivo)
        dir_error = os.getcwd() + "/reportes/Salidas/" + afp + "/errores_" + afp+"_"+fecha_archivo
        archivo_res = open(dir_res, 'w')
        archivo_error = open(dir_error, "w")

        archivo_res.writelines(
            "id_sil;rut;dig_rut;rut_pagador;dv_pagador;folio;periodo_remuneraciones;fecha_pago;dias_subsidio;fecha_ini_mov;fecha_fin_mov;cot_obligatoria;renta_imponible;cod_afp_pago;cod_afp_vigente;tipo_pago\n")

        for con in salida_final:
            id_sil = str(con[0]).strip()
            if id_sil == 'None':
                id_sil = ''
            rut = str(con[1]).strip()
            if rut == 'None':
                rut = ''
            dig_rut = str(con[2]).strip()
            if dig_rut == 'None':
                dig_rut = ''
            rut_pagador = str(con[3]).strip()
            if rut_pagador == 'None':
                rut_pagador = ''
            dv_pagador = str(con[4]).strip()
            if dv_pagador == 'None':
                dv_pagador = ''
            folio = str(con[5]).strip()
            if folio == 'None':
                folio = ''
            periodo_remuneraciones = str(con[6]).strip()
            if periodo_remuneraciones == 'None':
                periodo_remuneraciones = ''
            fecha_pago = str(con[7]).strip()
            if fecha_pago == 'None' or fecha_pago == '1900-01-01':
                fecha_pago = ''
            dias_subsidio = str(con[8]).strip()
            if dias_subsidio == 'None':
                dias_subsidio = ''
            fecha_ini_mov = str(con[9]).strip()
            if fecha_ini_mov == 'None' or fecha_ini_mov == '1900-01-01':
                fecha_ini_mov = ''
            fecha_fin_mov = str(con[10]).strip()
            if fecha_fin_mov == 'None' or fecha_fin_mov == '1900-01-01':
                fecha_fin_mov = ''
            cot_obligatoria = str(con[11]).strip()
            if cot_obligatoria == 'None':
                cot_obligatoria = ''
            renta_imponible = str(con[12]).strip()
            if renta_imponible == 'None':
                renta_imponible = ''
            cod_afp_pago = str(con[13]).strip()
            if cod_afp_pago == 'None':
                cod_afp_pago = ''
            cod_afp_vigente = str(con[14]).strip()
            if cod_afp_vigente == 'None':
                cod_afp_vigente = ''
            tipo_pago = str(con[15]).strip()
            if tipo_pago == 'None':
                tipo_pago = ''

            archivo_res.writelines(id_sil+";"+rut+";"+dig_rut+";"+rut_pagador+";"+dv_pagador+";"+folio+";"+periodo_remuneraciones+";"+fecha_pago+";" +
                                   dias_subsidio+";"+fecha_ini_mov+";"+fecha_fin_mov+";"+cot_obligatoria+";"+renta_imponible+";"+cod_afp_pago+";"+cod_afp_vigente+";"+tipo_pago+"\n")

        archivo_error.writelines("rut;dv;glosa_retorno\n")
        for err in salida_errores:
            rut = str(err[0]).strip()
            dv = str(err[1]).strip()
            glosa_retorno = str(err[2]).strip()

            archivo_error.writelines(rut+";"+dv+";"+glosa_retorno+"\n")

        datos = [afp, fecha_archivo]
        archivos = [dir_res, dir_error]

        return datos, archivos

    except Exception as er:
        print("No pasooo ")
        estado = False
        informacion_error = str(er)
        enviar_mail_con_error(str(er))
        logger(str(er))
    finally:
        actualizar_bitacora(id_instancia=id_instancia, estado=estado,
                            nombre_funcion="consulta_datos", error=informacion_error)


def comprimir_archivos(afp, fecha_archivo, archivos):
    logger(f'Comprimiendo archivos  {str(archivos)} de AFP: {afp}')
    estado = True
    informacion_error = ""
    id_instancia = insertar_bitacora("10405")
    try:
        fecha_split = fecha_archivo.split(".")
        directorio = os.getcwd() + "/reportes/Salidas/" + afp + \
            "/RESSIL_" + afp+"_"+str(fecha_split[0])+".zip"
        fantasy_zip = zipfile.ZipFile(directorio, 'w')

        for ar in archivos:
            fantasy_zip.write(ar, os.path.relpath(
                ar, directorio), compress_type=zipfile.ZIP_DEFLATED)

        fantasy_zip.close()

        return afp, directorio
    except Exception as e:
        estado = False
        informacion_error = str(e)
        logger(str(e))
        enviar_mail_con_error(str(e))

    finally:
        actualizar_bitacora(id_instancia=id_instancia, estado=estado,
                            nombre_funcion="comprimir_archivos", error=informacion_error)
