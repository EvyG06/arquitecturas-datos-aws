#!/usr/bin/env bash
# Evidencias de SOLO LECTURA del data lake. No crea ni modifica nada en AWS.
#   ./data-lake/evidencias/estado.sh                 todas las secciones, con pausa para cada captura
#   ./data-lake/evidencias/estado.sh 10              solo la seccion 10 (siempre con dos digitos)
#   SIN_PAUSA=1 ./data-lake/evidencias/estado.sh     sin pausas, solo genera los .txt
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
OUT=evidencias/dl/estado
source data-lake/evidencias/_lib.sh

s_identidad() {
  echo "Perfil por defecto (admin), crea la infraestructura"
  aws sts get-caller-identity --query Arn --output text
  echo
  echo "Perfil pipeline, ejecuta el codigo con el rol de minimo privilegio"
  aws sts get-caller-identity --profile pipeline --query Arn --output text
  echo
  echo "El mismo rol visto desde Python (boto3)"
  py -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
}

s_bucket() {
  local b="$DL_BUCKET"
  echo "Bucket $b"
  echo "  cifrado por defecto    $(aws s3api get-bucket-encryption --bucket "$b" --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm' --output text)"
  echo "  propiedad de objetos   $(aws s3api get-bucket-ownership-controls --bucket "$b" --query 'OwnershipControls.Rules[0].ObjectOwnership' --output text)"
  echo "  versionado             $(aws s3api get-bucket-versioning --bucket "$b" --query Status --output text)"
  echo "  etiquetas              $(aws s3api get-bucket-tagging --bucket "$b" --query 'TagSet[].join(`"="`, [Key, Value])' --output text)"
  echo
  echo "Bloqueo de acceso publico"
  aws s3api get-public-access-block --bucket "$b" --query PublicAccessBlockConfiguration --output table
  echo "Ciclo de vida"
  aws s3api get-bucket-lifecycle-configuration --bucket "$b" --query 'Rules[].[ID, Status]' --output table
  echo "Politica del bucket"
  aws s3api get-bucket-policy --bucket "$b" --query Policy --output text | python -m json.tool
  echo
  echo "Prueba del control, peticion firmada pero por HTTP sin TLS (debe fallar con explicit deny)"
  aws s3 ls "s3://$b" --endpoint-url "http://s3.${AWS_REGION}.amazonaws.com" 2>&1 | tail -1
}

s_rol() {
  local r=arq-datos-pipeline key
  echo "Rol $r, identidades que pueden asumirlo"
  aws iam get-role --role-name "$r" --query 'Role.AssumeRolePolicyDocument.Statement[].[Sid, Principal.Service || Principal.AWS]' --output table
  echo "Politicas inline del rol  $(aws iam list-role-policies --role-name "$r" --query PolicyNames --output text)"
  aws iam get-role-policy --role-name "$r" --policy-name dl-access \
    --query 'PolicyDocument.Statement[].[Sid, join(`", "`, to_array(Action))]' --output table
  echo "Prueba 1, el pipeline intenta borrar un original de Bronze (debe fallar)"
  key=$(aws s3api list-objects-v2 --bucket "$DL_BUCKET" --prefix bronze/ --profile pipeline \
    --query 'Contents[?ends_with(Key, `"feed.xml"`)] | [0].Key' --output text)
  aws s3 rm "s3://$DL_BUCKET/$key" --profile pipeline 2>&1 | tail -1
  echo
  echo "Prueba 2, el pipeline intenta listar todos los buckets de la cuenta (debe fallar)"
  aws s3 ls --profile pipeline 2>&1 | tail -1
}

s_fuentes() {
  echo "Configuracion versionada en data-lake/config/sources.json"
  python -m json.tool data-lake/config/sources.json
  echo
  echo "Estado actual de cada feed"
  python -c 'import json; [print(s["source_id"], s["url"]) for s in json.load(open("data-lake/config/sources.json"))]' |
    while read -r id url; do
      curl -sS -A 'arq-datos-eia/0.1 (proyecto academico)' -o /tmp/evidencia_feed.xml \
        -w "  $id http=%{http_code} tipo=%{content_type} bytes=%{size_download}" "$url"
      echo " items=$(grep -o '<item>' /tmp/evidencia_feed.xml | wc -l | tr -d ' ')"
    done
}

s_bronze_lotes() { py -m dl.evidence bronze; }

s_bronze_archivos() {
  aws s3 ls "s3://$DL_BUCKET/bronze/" --recursive --profile pipeline
  echo
  py -m dl.evidence bronze --meta manual__primera | sed -n '/Registro del lote/,$p'
}

s_silver() { py -m dl.evidence silver; }
s_muestra() { py -m dl.evidence muestra --n 8; }

s_cuarentena() {
  echo "Registros rechazados por Silver, un archivo por lote"
  aws s3 ls "s3://$DL_BUCKET/quarantine/" --recursive --profile pipeline || echo "  (sin rechazos todavia)"
}

s_comprehend() { py -m dl.evidence comprehend; }
s_gold() { py -m dl.evidence gold; }

s_glue_tablas() {
  echo "Base de datos  $(aws glue get-database --name arq_dl --profile pipeline --query 'Database.[Name, Description]' --output text)"
  aws glue get-tables --database-name arq_dl --profile pipeline \
    --query 'TableList[].{tabla: Name, formato: Parameters.classification, particion: join(`" "`, PartitionKeys[].[Name, Type][]), ubicacion: StorageDescriptor.Location}' \
    --output table
}

s_glue_columnas() {
  local t
  for t in silver_news gold_entity_daily; do
    echo "$t  serde $(aws glue get-table --database-name arq_dl --name "$t" --profile pipeline --query 'Table.StorageDescriptor.SerdeInfo.SerializationLibrary' --output text)"
    aws glue get-table --database-name arq_dl --name "$t" --profile pipeline \
      --query 'Table.StorageDescriptor.Columns[].[Name, Type]' --output text | column -t | sed 's/^/    /'
    echo
  done
}

s_glue_particiones() {
  py -m dl.evidence particiones
  echo
  echo "Ubicacion registrada de cada particion de silver_news"
  aws glue get-partitions --database-name arq_dl --table-name silver_news --profile pipeline \
    --query 'Partitions[].[Values[0], StorageDescriptor.Location]' --output text | sort
}

s_athena() {
  sed 's/^/    /' "$1"
  py -m dl.athena "$1"
}

s_bytes() {
  py -m dl.athena data-lake/sql/07a_con_filtro_de_particion.sql data-lake/sql/07b_sin_filtro_de_particion.sql |
    grep -E '^== |bytes escaneados'
}

s_pruebas() { python -m pytest data-lake/tests -v -p no:cacheprovider; }

paso 01 identidad "Identidades y separacion de funciones" s_identidad
paso 02 bucket "Bucket unico del data lake y sus controles" s_bucket
paso 03 rol "Rol de pipeline con minimo privilegio" s_rol
paso 04 fuentes "Fuentes RSS seleccionadas" s_fuentes
paso 05 bronze_lotes "Bronze, registro por lote y fuente" s_bronze_lotes
paso 06 bronze_archivos "Bronze, originales y metadatos del lote" s_bronze_archivos
paso 07 silver_manifiesto "Silver, manifiesto vigente y lotes publicados" s_silver
paso 08 silver_muestra "Silver, noticias normalizadas con fecha UTC y valor original" s_muestra
paso 09 cuarentena "Cuarentena de registros invalidos" s_cuarentena
paso 10 comprehend "Resultados de Amazon Comprehend" s_comprehend
paso 11 gold "Gold, indicadores por fecha" s_gold
paso 12 glue_tablas "Glue Data Catalog, base y tablas" s_glue_tablas
paso 13 glue_columnas "Glue Data Catalog, columnas, tipos y formato" s_glue_columnas
paso 14 glue_particiones "Glue Data Catalog, particiones" s_glue_particiones
n=15
for f in data-lake/sql/*.sql; do
  paso "$n" "athena_$(basename "$f" .sql)" "Athena, $(basename "$f")" s_athena "$f"
  n=$((n + 1))
done
paso 23 athena_bytes "Athena, bytes escaneados con y sin filtro de particion" s_bytes
paso 24 pruebas "Pruebas automatizadas del pipeline" s_pruebas
