setup_hive_tables() {
  local dir=${1:-$BASE_DIR/ddl}

  for f in $dir/*.hql
  do
    hive -f $f
  done
}
