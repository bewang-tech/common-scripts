create_tables() {
  local tables="$@"

  local query=""
  if [ -z "$tables" ]; then
    for t in $DDL_DIR/*.hql; do
      query="$query $(cat $t)"
    done
  else 
    for t in ${tables}; do
      query="$query $(cat $DDL_DIR/$t.hql)"
    done
  fi

  info "Creating teradata tables ..."
  hive -e "$query"

  info "Updating Impala ..."
  impala -q "invalidate metadata"
}
