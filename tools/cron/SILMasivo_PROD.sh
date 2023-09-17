#!/usr/bin/bash

echo "Activando entorno virtual"
source /previred/SIL_Masivo/.env/bin/activate

echo "Dirijiendo a directorio /previred/SIL_Masivo"
cd /previred/SIL_Masivo

echo "Iniciando script SIL Masivo"
python3 main.py

echo "Desactivando entorno virtual"
deactivate
