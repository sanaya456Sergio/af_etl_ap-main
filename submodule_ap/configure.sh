#!/usr/bin/env bash
set -euo pipefail

# Activación de modo estricto:
# -e  → Detiene el script si un comando falla
# -u  → Falla si se intenta usar una variable no definida
# -o pipefail → Falla si alguna parte de una tubería falla
echo "🔐 Activado modo estricto de bash: 'set -euo pipefail'"

# Rutas destino relativas al submódulo
DEST_DAGS=../../dags
DEST_MODELOS=../../otl/modelos
DEST_ETL=../../otl/etl

echo "📁 Definiendo rutas de destino:"
echo "   - DAGs:        $DEST_DAGS"
echo "   - Modelos:     $DEST_MODELOS"
echo "   - ETL scripts: $DEST_ETL"

# Crea las carpetas destino si no existen
echo "🛠️  Creando carpetas de destino si no existen..."
mkdir -p "$DEST_DAGS" "$DEST_MODELOS" "$DEST_ETL"
echo "✅ Directorios verificados/creados exitosamente"

echo "⚙️  Iniciando configuración del submódulo ETL de Áreas Protegidas..."

# Copia la carpeta `dag_ap` al destino de DAGs
echo "📤 Copiando carpeta 'dag_ap' a $DEST_DAGS/"
cp -a dag_ap "$DEST_DAGS"/
echo "✅ 'dag_ap' copiado correctamente"

# Copia la carpeta `modelo_ap` al destino de modelos
echo "📤 Copiando carpeta 'modelo_ap' a $DEST_MODELOS/"
cp -a modelo_ap "$DEST_MODELOS"/
echo "✅ 'modelo_ap' copiado correctamente"

# Copia la carpeta `etl_ap` al destino de scripts ETL
echo "📤 Copiando carpeta 'etl_ap' a $DEST_ETL/"
cp -a etl_ap "$DEST_ETL"/
echo "✅ 'etl_ap' copiado correctamente"

echo "🏁 Submódulo ETL de Áreas Protegidas configurado correctamente."