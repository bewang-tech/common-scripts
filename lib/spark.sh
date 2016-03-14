require common/cdh

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
  if [ "$conf_name" = "application.conf" ]; then
    echo $CONF_DIR/$app_conf 
  else
    echo $app_conf --app-conf $conf_name
  fi
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

# This is for debug
# --conf spark.driver.extraJavaOptions='-agentlib:jdwp=transport=dt_socket,server=y,address=4444,suspend=y'

spark_hive() {
  local app_name=$1
  shift 1

  read conf_file conf_opt <<< $(handle_conf)

  local files=$HIVE_CONF_DIR/hive-site.xml,$conf_file
  if [ -n "$MODULE_FILES" ]; then
    files=$files,$MODULE_FILES
  fi

  local driver_cp=$(datanucleus_jars):$GUAVA_CLASSPATH
  if [ -n "$MODULE_DRIVER_CP_JARS" ]; then
    driver_cp=$driver_cp:$MODULE_DRIVER_CP_JARS
  fi

  local exec_extra=""
  if [ -n "$MODULE_EXEC_CP_JARS" ]; then
    exec_extra="--conf spark.executor.extraClassPath=$MODULE_EXEC_CP_JARS"
  fi

  local num_executors=${NUM_EXECUTORS:-4}
  local num_cores=${NUM_EXECUTOR_CORE:-2}
  local exec_mem=${EXECUTOR_MEM:-1G}

  # for yarn-class mode, we need to use --driver-class-path to put
  # jsch and guava jar before the others in the class path
  # so that the lower version of jsch and guava from hadoop jars
  # can be overrided.
  spark_yarn_submit \
    --num-executors $num_executors \
    --executor-cores $num_cores \
    --executor-memory $exec_mem \
    --name $app_name \
    --conf spark.sql.hive.metastore.version=0.13.1 \
    --conf spark.sql.hive.metastore.jars=hive-site.xml:$HIVE_LIB_DIR/* \
    --conf spark.sql.caseSensitive=false \
    --driver-class-path $driver_cp $exec_extra \
    --jars $MODULE_LIB_JARS \
    --files $files\
    --class $MODULE_APP_CLASS \
    $MODULE_JAR "$@" $conf_opt
}

spark_hive_shell() {
  read conf_file conf_opt <<< $(handle_conf)

  local exec_extra=""
  if [ -n "$MODULE_EXEC_CP_JARS" ]; then
    exec_extra="--conf spark.executor.extraClassPath=$MODULE_EXEC_CP_JARS"
  fi

  local num_executors=${SH_NUM_EXECUTORS:-8}
  local num_cores=${SH_NUM_EXECUTOR_CORE:-3}
  local exec_mem=${SH_EXECUTOR_MEM:-6G}

  $SPARK_SHELL \
    --master yarn \
    --num-executors $num_executors \
    --executor-cores $num_cores \
    --executor-memory $exec_mem \
    --conf spark.sql.hive.metastore.version=0.13.1 \
    --conf spark.sql.hive.metastore.jars=$(hive_metastore_classpath) \
    --conf spark.driver.extraClassPath=$GUAVA_CLASSPATH $exec_extra \
    --conf spark.sql.caseSensitive=false \
    --conf spark.app.config=$conf_file \
    --jars $MODULE_JAR,$MODULE_LIB_JARS
}
