import datetime


def logger(mensaje):
    now = datetime.datetime.now()
    fecha = str(now.strftime("%Y_%m_%d %H:%M:%S"))

    print("["+str(fecha)+"]: " + str(mensaje))
