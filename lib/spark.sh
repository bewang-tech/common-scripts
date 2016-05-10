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

GUAVA_JAR=${GUAVA_JAR:-guava-15.0.jar}

SPARK_EXTRA_OPTIONS=${SPARK_EXTRA_OPTIONS:-}

setup_guava_path() {
  local cdh_path=$CDH_JARS/$GUAVA_JAR
  local lib_path=$LIB_DIR/$GUAVA_JAR

  if [ -f "$cdh_path" ]; then
    GUAVA_CLASSPATH=$cdh_path
  elif [ -f "$lib_path" ]; then
    GUAVA_CLASSPATH=./$GUAVA_JAR
    GUAVA_LOCAL_PATH=$lib_path
  else
    error "$GUAVA_JAR does not exist at both $cdh_path and $lib_path."
    exit -1
  fi
}

setup_guava_path

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

  local exec_extra_cp=$GUAVA_CLASSPATH
  if [ -n "$MODULE_EXEC_CP_JARS" ]; then
    exec_extra_cp="$exec_extra_cp:$MODULE_EXEC_CP_JARS"
  fi

  local num_executors=${EXECUTORS:-12}
  local num_cores=${EXECUTOR_CORES:-3}
  local exec_mem=${EXECUTOR_MEM:-6G}

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
    --conf spark.executor.extraClassPath=$exec_extra_cp \
    --driver-class-path $driver_cp \
    --jars $MODULE_LIB_JARS \
    --files $files\
    --class $MODULE_APP_CLASS $SPARK_EXTRA_OPTIONS \
    $MODULE_JAR "$@" $conf_opt
}

spark_hive_shell() {
  read conf_file conf_opt <<< $(handle_conf)

  local exec_extra_cp="$GUAVA_CLASSPATH"
  if [ -n "$MODULE_EXEC_CP_JARS" ]; then
    exec_extra_cp="--conf spark.executor.extraClassPath=$MODULE_EXEC_CP_JARS"
  fi

  local num_executors=${EXECUTORS:-12}
  local num_cores=${EXECUTOR_CORES:-3}
  local exec_mem=${EXECUTOR_MEM:-6G}

  if [[ $GUAVA_CLASSPATH =~ ^\. ]]; then
    SPARK_EXTRA_OPTIONS="$SPARK_EXTRA_OPTIONS --files $GUAVA_LOCAL_PATH"
  fi

  $SPARK_SHELL \
    --master yarn \
    --num-executors $num_executors \
    --executor-cores $num_cores \
    --executor-memory $exec_mem \
    --conf spark.sql.hive.metastore.version=0.13.1 \
    --conf spark.sql.hive.metastore.jars=$(hive_metastore_classpath) \
    --conf spark.sql.caseSensitive=false \
    --conf spark.app.config=$conf_file \
    --conf spark.executor.extraClassPath=$exec_extra_cp \
    --driver-class-path $GUAVA_CLASSPATH \
    --jars $MODULE_JAR,$MODULE_LIB_JARS $SPARK_EXTRA_OPTIONS "$@"
}

spark_streaming() {
  local options="$@"

  read conf_file conf_opt <<< $(handle_conf)

  local files=$conf_file
  if [ -n "$MODULE_FILES" ]; then
    files=$files,$MODULE_FILES
  fi

  spark_yarn_submit \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --files $files \
    --jars $MODULE_LIB_JARS $options \
    $MODULE_JAR $conf_opt
}
