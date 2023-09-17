from config.config import FROM_USER, PASS, PORT, SMTP_SERVER, SUBJECT, USER
from config.log import logger

from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import smtplib
import os


email_encargado_procesos_python = '***************'
html_path = os.getcwd() + '/tools/html/message_error.html'


def enviar_mail_con_error(msjError=''):
    try:
        message = open(html_path, 'r', encoding='utf8').read().format(msjError=str(
            msjError).replace("'", ""), date=datetime.today().strftime('%m/%d/%Y'))
        destinations = email_encargado_procesos_python

        content = MIMEMultipart("alternative")
        content['From'] = FROM_USER
        content['To'] = destinations
        content['Subject'] = SUBJECT
        content.attach(MIMEText(message, "html"))

        conectar_server_mail(content, FROM_USER, destinations)

    except (FileNotFoundError, AttributeError, KeyError) as e:
        logger('Error en función (enviar_mail_con_error) ' + str(e))


def conectar_server_mail(content, from_u, to_u):
    logger('Iniciando conexión de EMAIL con el servidor ' + SMTP_SERVER)
    try:

        with smtplib.SMTP(SMTP_SERVER, PORT) as smtp:
            logger('Esperando conexión con el servidor ' + SMTP_SERVER)
            smtp.connect(SMTP_SERVER, PORT)
            logger('Conectado a  ' + SMTP_SERVER + '.........')

            if sys.platform.startswith('win32'):
                smtp.ehlo()
                smtp.starttls()

            logger('Iniciando sesión en ' + from_u + '.........')
            smtp.login(USER, PASS)
            logger('Sesión iniciada con éxito en' + from_u)

            smtp.sendmail(from_addr=from_u, to_addrs=to_u.split(","), msg=content.as_string())
            logger('Correo de mensaje de error enviado con éxito')

            smtp.quit()
            logger('Fin del proceso, terminado con error.')
            quit()

    except (smtplib.SMTPAuthenticationError, smtplib.SMTPResponseException) as e:
        logger('Error al enviar email.  ' + str(e))
