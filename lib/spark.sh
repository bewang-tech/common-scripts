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
handle_conf() {
  local conf_name=$(basename $APP_CONF)
  local arg_conf=""
  if [ "$conf_name" != "application.conf" ]; then
    arg_conf="--conf $conf_name"
  fi
  echo $APP_CONF "$arg_conf"
}
