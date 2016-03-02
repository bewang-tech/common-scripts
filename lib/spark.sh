yarn_num_nodes() {
  yarn node -list | head -1 | awk -F : '{print $2}'
}

spark_yarn_cluster() {
  local args=(
    --master yarn 
    --deploy-mode cluster 
    --num-executors $(yarn_num_nodes))
  echo "${args[@]}"
}

read_spark_conf() {
   cat $1 | sed 's/^/--conf /' | tr '\n' ' '
}

to_spark_jars() {
  tr ":" ","
}

# handle configuration file
# if the user specified APP_CONF, return
#  <the config file> <--app-conf conf_name>
# otherwise
#  <the config file name>
handle_conf() {
  local app_conf=${APP_CONF:-application.conf}
  local conf_name=$(basename $app_conf)
  local arg_conf=""
  if [ "$conf_name" != "application.conf" ]; then
    arg_conf="--app-conf $conf_name"
  fi
  echo $CONF_DIR/$app_conf "$arg_conf"
}

SPARK_VERSION=rhap1.6.0
SPARK_RHAP=~/spark-rhap/spark-${SPARK_VERSION}

SPARK_SUBMIT=$SPARK_RHAP/bin/spark-submit
SPARK_SHELL=$SPARK_RHAP/bin/spark-shell
SPARK_SQL=$SPARK_RHAP/bin/spark-sql

SPARK_CONF=/etc/spark/conf/spark-defaults.conf

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf

CDH_LIB_DIR=$(cdh_lib_dir)
CDH_JARS=$CDH_LIB_DIR/../jars

HIVE_LIB_DIR=$CDH_LIB_DIR/hive/lib

GUAVA_CLASSPATH=$CDH_JARS/guava-15.0.jar

hive_metastore_classpath() {
  echo "$HIVE_CONF_DIR:$HIVE_LIB_DIR/*"
}

datanucleus_jars() {
  find $HIVE_LIB_DIR -name "datanucleus*.jar" | tr '\n' :
}

spark_yarn_submit() {
  $SPARK_SUBMIT \
    --master yarn-cluster \
    --properties-file $SPARK_CONF "$@"
}

spark_hive() {
  local app_name=$1
  shift 1

  read conf_file conf_opt <<< $(handle_conf)

  spark_yarn_submit \
    --num-executors 3 \
    --executor-cores 1 \
    --executor-memory 1G \
    --name $app_name \
    --conf spark.sql.hive.metastore.version=0.13.1 \
    --conf spark.sql.hive.metastore.jars=$(hive_metastore_classpath) \
    --conf spark.driver.extraClassPath=$GUAVA_CLASSPATH \
    --conf spark.sql.caseSensitive=false \
    --driver-class-path $(datanucleus_jars) \
    --jars $MODULE_LIB_JARS \
    --files $conf_file \
    --class $MODULE_APP_CLASS \
    $MODULE_JAR "$@" $conf_opt
}

spark_hive_shell() {
  $SPARK_SHELL \
    --master yarn \
    --num-executors 8 \
    --executor-cores 3 \
    --executor-memory 6G \
    --conf spark.sql.hive.metastore.version=0.13.1 \
    --conf spark.sql.hive.metastore.jars=$(hive_metastore_classpath) \
    --conf spark.driver.extraClassPath=$GUAVA_CLASSPATH \
    --conf spark.sql.caseSensitive=false \
    --jars $MODULE_JAR
}
