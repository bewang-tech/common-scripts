CREDS_FILE=${CREDS_FILE:-$CONF_DIR/teradata-creds.sh}
if [ -f "$CREDS_FILE" ]; then
  source $CREDS_FILE
fi

EXPORT_QUEUE=${EXPORT_QUEUE:-TD-Import}
TERADATA_SERVER=${TERADATA_SERVER:-ribicop1.internal.rhapsody.com}

TERADATA_JDBC_JARS=$(find -L $LIB_DIR/tdch -name "tdgssconfig.jar" -or -name "terajdbc4.jar")
TERADATA_CONNECTOR_JAR=$(find -L $LIB_DIR/tdch -name "teradata-connector-*.jar")

CDH_LIB=$(cdh_lib_dir)
HIVE_HOME=${HIVE_HOME:-${CDH_LIB}/hive}
HCAT_LIB_JARS=(
  ${CDH_LIB}/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar
  $HIVE_HOME/lib/hive-cli.jar
  $HIVE_HOME/lib/hive-exec.jar
  $HIVE_HOME/lib/hive-metastore.jar
  $HIVE_HOME/lib/libfb303-0.9.0.jar
  $HIVE_HOME/lib/libthrift-0.9.0-cdh5-2.jar
  $HIVE_HOME/lib/jdo-api-3.0.1.jar
  )

LIB_JARS=$(echo ${TERADATA_JDBC_JARS} ${HCAT_LIB_JARS[@]} | tr ' ' ',')

export HADOOP_CLASSPATH=$(echo ${TERADATA_JDBC_JARS} | tr ' ' ':'):$(hcat -classpath)

export_options() {
  local name=$1
  local job_name=${EXPORT_JOB_NAME:-export-${name}}
  echo "-Dmapreduce.job.name=$job_name -Dmapreduce.job.queuename=${EXPORT_QUEUE}"
}

# the caller of this function must provide TERADATA_DB
tdch_export() {
  hadoop jar ${TERADATA_CONNECTOR_JAR} \
    com.teradata.connector.common.tool.ConnectorExportTool \
    $EXPORT_OPTS \
    -libjars ${LIB_JARS} \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://$TERADATA_SERVER/DATABASE=$TERADATA_DB,CHARTSET=UTF8 \
    -username $TERADATA_USER \
    -password $TERADATA_PASSWD \
    -jobtype hcat \
    -method internal.fastload "$@"
}

export_table() {
  local hive_table_name=$1
  local td_table_name=$2
  shift 2

  IFS='.' read hive_db hive_table <<<$(echo $hive_table_name)
  IFS='.' read td_db td_table <<<$(echo $td_table_name)

  TERADATA_DB=$td_db
  EXPORT_OPTS=$(export_options $hive_table_name)
  tdch_export \
    -sourcetable $hive_table \
    -sourcedatabase $hive_db \
    -targettable $td_table "$@"
}

export_table_partitions() {
  local hive_table=$1
  local td_table=$2
  local add_query=$3
  local drop_query=$4
  shift 4

  info "Dropping the existing partitions of ${hive_table} ..."
  if [ -z "$drop_query" ]; then
    info "No partition exists."
  else
    impala -q "$drop_query"
  fi

  info "Adding the partitions being exported into ${hive_table} ..."
  impala -q "$add_query"

  info "Loading ${hive_table} to Teradata ${td_table} ..."
  export_table ${hive_table} ${td_table} "$@"
}
