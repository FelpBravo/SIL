#!/usr/bin/bash

echo "Activando entorno virtual"
source /home/app_proc_qa/PruebasServerproductivoPython/SIL_Masivo/.env/bin/activate

echo "Dirijiendo a directorio /home/app_proc_qa/PruebasServerproductivoPython/SIL_Masivo"
cd /home/app_proc_qa/PruebasServerproductivoPython/SIL_Masivo

echo "Iniciando script SIL Masivo"
python main.py

echo "Desactivando entorno virtual"
deactivate
