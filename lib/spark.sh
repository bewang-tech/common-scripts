require common/cdh

yarn_num_nodes() {
  yarn node -list | head -1 | awk -F : '{print $2}'
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

SPARK_RHAP_HOME=${SPARK_RHAP_HOME:-/opt/spark-rhap}
SPARK_VERSION=${SPARK_VERSION:-rhap2.0.1}
SPARK_RHAP=${SPARK_RHAP_HOME}/spark-${SPARK_VERSION}

SPARK_SUBMIT=$SPARK_RHAP/bin/spark-submit
SPARK_SHELL=$SPARK_RHAP/bin/spark-shell
SPARK_SQL=$SPARK_RHAP/bin/spark-sql

SPARK_CONF=/etc/spark/conf/spark-defaults.conf

HDFS_NAME_SERVICE=${HDFS_NAME_SERVICE:-$(hdfs getconf -confKey dfs.nameservices)}
SPARK_RHAP_YARN_ARCHIVE=hdfs://${HDFS_NAME_SERVICE}/bi/spark-rhap/spark-${SPARK_VERSION}.zip 

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
export HIVE_CONF_DIR=${HIVE_CONF_DIR:-/etc/hive/conf}

CDH_LIB_DIR=$(cdh_lib_dir)
CDH_JARS=$CDH_LIB_DIR/../jars

export LD_LIBRARY_PATH=$CDH_LIB_DIR/hadoop/lib/native

HIVE_LIB_DIR=$CDH_LIB_DIR/hive/lib


SPARK_EXTRA_OPTIONS=${SPARK_EXTRA_OPTIONS:-}

# spark-cassandra-connector uses some methods of guava only in 16.0.1, while Spark 2.0.1 use 14.0.1 and 
# CDH 5.3.2 uses 11.0.2.
#
# To make the applications using spark-cassandra-connector running correctly, we need to put guava-16.0.1 jar
# at the beginning of both driver class path and executor class paths so that guava-16.0.1.jar will be used.
#
# If the guava jar file can be found in $LIB_DIR, it is included in MODULE_LIB_JAR and will be shipped to the 
# executor YARN container. The classpath of driver (spark-submit) and executors for guava is `./guava-16.0.1.jar`.
#
# For spark-shell, the driver runs on the client, not in YARN node manager's container. We should
# use the local path in `--driver-class-path`.
#
GUAVA_JAR=${GUAVA_JAR:-guava-16.0.1.jar}

# This function set two ENV vars: GUAVA_CLASSPATH and GUAVA_LOCAL_PATH. If GUAVA_JAR is in LIB_DIR,
# use it. Otherwise try to find in CDH distribution.
setup_guava_path() {
  local cdh_path=$CDH_JARS/$GUAVA_JAR
  local lib_path=$LIB_DIR/$GUAVA_JAR

  if [ -f "$lib_path" ]; then
    GUAVA_CLASSPATH=./$GUAVA_JAR
    GUAVA_LOCAL_PATH=$lib_path
  elif [ -f "$cdh_path" ]; then
    GUAVA_CLASSPATH=$cdh_path
    GUAVA_LOCAL_PATH=$cdh_path
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
    --master yarn \
    --deploy-mode cluster \
    --properties-file $SPARK_CONF "$@"
}

# This is for debug
# --conf spark.driver.extraJavaOptions='-agentlib:jdwp=transport=dt_socket,server=y,address=4444,suspend=y'

spark_hive_run() {
  local app_name_args=""
  if [ -n "$APP_NAME" ]; then
    app_name_args="--name $APP_NAME"
  fi

  read conf_file conf_opt <<< $(handle_conf)

  local files=$HIVE_CONF_DIR/hive-site.xml,$conf_file
  if [ -n "$MODULE_FILES" ]; then
    files=$files,$MODULE_FILES
  fi

  local driver_cp=$(datanucleus_jars):$GUAVA_LOCAL_PATH
  if [ -n "$MODULE_DRIVER_CP_JARS" ]; then
    driver_cp=$driver_cp:$MODULE_DRIVER_CP_JARS
  fi

  local exec_extra_cp=$GUAVA_CLASSPATH
  if [ -n "$MODULE_EXEC_CP_JARS" ]; then
    exec_extra_cp="$exec_extra_cp:$MODULE_EXEC_CP_JARS"
  fi

  local num_executors=${EXECUTORS:-16}
  local num_cores=${EXECUTOR_CORES:-3}
  local exec_mem=${EXECUTOR_MEM:-8G}

  # for yarn-class mode, we need to use --driver-class-path to put
  # jsch and guava jar before the others in the class path
  # so that the lower version of jsch and guava from hadoop jars
  # can be overrided.
  spark_yarn_submit \
    --num-executors $num_executors \
    --executor-cores $num_cores \
    --executor-memory $exec_mem $app_name_args \
    --conf spark.sql.hive.metastore.version=0.13.1 \
    --conf spark.sql.hive.metastore.jars=hive-site.xml:$HIVE_LIB_DIR/* \
    --conf spark.sql.caseSensitive=false \
    --conf spark.executor.extraClassPath=$exec_extra_cp \
    --conf spark.yarn.archive=$SPARK_RHAP_YARN_ARCHIVE \
    --conf spark.driver.extraJavaOptions=-Djava.library.path=$LD_LIBRARY_PATH \
    --conf spark.executor.extraJavaOptions=-Djava.library.path=$LD_LIBRARY_PATH \
    --driver-class-path $driver_cp \
    --jars $MODULE_LIB_JARS \
    --files $files\
    --class $MODULE_APP_CLASS $SPARK_EXTRA_OPTIONS \
    $MODULE_JAR "$@" $conf_opt
}

spark_hive() {
  local app_name=$1
  shift 1

  APP_NAME=$app_name spark_hive_run "$@"
}

# Find the position of delimiter '--' in the arguments.
# 
# - no delimiter: return $# + 1
# - '--' is the last argument: return $# 
# - with '--' following some arguments: 
#   return the pos of delimiter. In this case, $pos must smaller than $#
#
pos_delimiter() {
  local pos=1
  until [ -z "$1" -o "$1" == "--" ]; do
    pos=$(( pos + 1 ))
    shift
  done
  echo $pos
}

build_args() {
  local args=""
  until [ -z "$1" ]; do
    if [ -z "$args" ]; then
      args="\"$1\""
    else
      args="$args, \"$1\""
    fi
    shift
  done
  echo $args
}

# Generate a Scala script running the specified scala function with the input arguments
# exit with 0 if everything is ok, or catch and dump the exception,
# then exit with -1 so that the shell will quit.
build_run_script() {
  local func=$1
  local args=$2
  echo "
try {
  $func(Array($args))
  sys.exit(0)
} catch {
  case e: Throwable =>
    e.printStackTrace
    sys.exit(-1)
}
  "
}

module_jars() {
  echo "$MODULE_JAR,$MODULE_LIB_JARS" | sed -e 's/^,\|,$//'
}

# allow to run a script then quit the shell with correct an exit status.
#
# Assume you want to run the function do_something in $LIB_DIR/script1.scala:
# script1.scala
#
# def do_something(args: Array[String]) = println(s"do something in spark shell ${args.mkString(",")}")
#
# The following command will load script1.scala and run do_something with the given arguments
#
# spark_hive_shell \
#   --name run-script-in-spark-shell \
#   -i $LIB_DIR/script1.scala \
#   -- do_something arg_1 arg_2 arg_3
#
# Notes:
#   - Give a Spark Application name using --name
#   - Use '-i' to load the script. If you have multiple script files, you can
#     load them using one '-i' with multiple file names separated with blank:
#
#     -i $LIB_DIR/script1.scala $LIB_DIR/script2.scala $LIB_DIR/script3.scala
#
#     or using multiple '-i'
#
#     -i $LIB_DIR/script1.scala \
#     -i $LIB_DIR/script2.scala \
#     -i $LIB_DIR/script3.scala \
#   
#   - Use '--' to separate the spark shell options and the script's function
#     and arguments. If an arguemnt is a string with spaces, you need to 
#     double quote it. for example
#     
#     -- do_something first_arg "second arg" "third arg"
#
#     When it runs, this function will run as below
#
#     do_something(Array("first_arg", "second_arg", "third arg"))
#
spark_hive_shell() {
  local pos=$(pos_delimiter "$@")

  read conf_file conf_opt <<< $(handle_conf)

  local driver_cp=$CONF_DIR:$GUAVA_CLASSPATH
  if [ -n "$MODULE_SH_DRIVER_CP_JARS" ]; then
    driver_cp=$driver_cp:$MODULE_SH_DRIVER_CP_JARS
  fi

  local exec_extra_cp="$GUAVA_CLASSPATH"
  if [ -n "$MODULE_EXEC_CP_JARS" ]; then
    exec_extra_cp=$exec_extra_cp:$MODULE_EXEC_CP_JARS
  fi

  local num_executors=${EXECUTORS:-16}
  local num_cores=${EXECUTOR_CORES:-3}
  local exec_mem=${EXECUTOR_MEM:-8G}

#  if [[ $GUAVA_CLASSPATH =~ ^\. ]]; then
#    SPARK_EXTRA_OPTIONS="$SPARK_EXTRA_OPTIONS --files $GUAVA_LOCAL_PATH"
#  fi

  local jars_opt=""
  local jars=$(module_jars)
  if [ -n "$jars" ]; then
    jars_opt="--jars $jars"
  fi

  function run_shell() {
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
      --conf spark.yarn.archive=$SPARK_RHAP_YARN_ARCHIVE \
      --conf spark.driver.extraJavaOptions=-Djava.library.path=$LD_LIBRARY_PATH \
      --conf spark.executor.extraJavaOptions=-Djava.library.path=$LD_LIBRARY_PATH \
      --driver-class-path $driver_cp $jars_opt "$@"
  }

  if [ $pos -lt $# ]; then
    local func=${@:$(($pos + 1)):1}
    local args=$(build_args "${@:$(($pos + 2))}")
    run_shell "${@:1:$(($pos - 1))}" \
      -i <(build_run_script $func "$args")
  else
    run_shell "$@"
  fi
}

spark_streaming() {
  local app_name=$1
  shift
  local options="$@"

  read conf_file conf_opt <<< $(handle_conf)

  local files=$conf_file
  if [ -n "$MODULE_FILES" ]; then
    files=$files,$MODULE_FILES
  fi

  local num_executors=${EXECUTORS:-3}
  local num_cores=${EXECUTOR_CORES:-1}
  local exec_mem=${EXECUTOR_MEM:-1G}

  spark_yarn_submit \
    --name $app_name \
    --num-executors $num_executors \
    --executor-cores $num_cores \
    --executor-memory $exec_mem \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --files $files \
    --class $MODULE_APP_CLASS \
    --jars $MODULE_LIB_JARS \
    $MODULE_JAR $conf_opt $options
}
