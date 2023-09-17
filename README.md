# SIL Masivo (Subsidio por Incapacidad Laboral)

SIL Masivo es un proyecto Python diseñado para ejecutarse en el sistema operativo Linux a través de un cronjob. Este proyecto se encarga de procesar archivos de entrada, realizar consultas a una base de datos, ejecutar Stored Procedures (SP), depositar archivos en un servidor SFTP y enviar informes por correo electrónico a instituciones correspondientes.

## Funcionamiento

El script se ejecuta automáticamente mediante un cronjob en días hábiles específicos. Primero, verifica si el día de la semana actual es el adecuado para su ejecución. Luego, realiza las siguientes tareas:

1. **Preparación**: Ejecuta un script SQL para preparar la base de datos.

2. **Obtención de Archivos**: Descarga archivos desde un servidor remoto SFTP. Selecciona archivos basados en la fecha y otros criterios.

3. **Inserción de Datos**: Procesa los archivos CSV obtenidos, insertando los datos en una base de datos local.

4. **Consulta de Datos**: Realiza consultas a la base de datos para obtener información adicional sobre los archivos procesados.

5. **Compresión de Archivos**: Comprime los archivos de salida en un archivo ZIP para su entrega a las instituciones correspondientes.

6. **Envío de Correo**: Envía correos electrónicos que contienen informes y archivos a las instituciones.

## Mejoras

El código original de este proyecto fue desarrollado por un colega junior y se han realizado varias mejoras y refactorizaciones para mejorar su eficiencia y funcionalidad.

## Requisitos

- Sistema operativo Linux
- Python 3.6 o superior
- Librería VirtualEnv 20.16 o superior

## Estructura del Proyecto

El proyecto está organizado en las siguientes carpetas:

- `config`: Configuración del proyecto.
- `functions`: Funciones y lógica del proyecto.
- `packages`: Librerías necesarias y guía de instalación.
- `reports`: Almacenamiento de archivos de entrada y salida.
- `tools`: Herramientas y scripts SQL utilizados en el proyecto.

## 

Nota Importante: Los archivos sensibles no han sido subidos al repositorio o han sido ocultados por razones de seguridad.

Estos archivos contienen información confidencial, como contraseñas o claves de acceso, y no deben estar disponibles públicamente en un repositorio de código abierto.

Este proyecto ha sido mejorado y mantenido por Felipe Eduardo Bravo Espinosa desde su versión original.

