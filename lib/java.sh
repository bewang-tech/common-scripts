require common/cdh

source $CDH_LIB_DIR/bigtop-utils/bigtop-detect-javahome

jar_conf_dir() {
  local app_conf_jar=$1
  shift
  local files=$@

  local temp_dir=$(mktemp -d --tmpdir)

  cp $files $temp_dir

  info "Creating $app_conf_jar for applicaton.conf ..."
  $JAVA_HOME/bin/jar cf $app_conf_jar -C $temp_dir . 

  rm -rf $tmp_dir
}

create_classpath_env() {
  local file=$1

  local classpath=$(cat $file)
  eval "__CLASSPATH__=$classpath"
  echo $__CLASSPATH__
}

get_classpath() {
  create_classpath_env $CONF_DIR/classpath
}

find_jar() {
  local dir=$1
  local artifact=$2
  find $dir -name "${artifact}*.jar"
}

find_jars() {
  local dir=$1
  shift

  for j in $@; do
    find_jar $dir $j
  done
}

to_classpath() {
  tr '\n' ':' | sed 's/:$//'
}

m2_classpath() {
  find_jars $BI_M2REPO "$@" | to_classpath
}
