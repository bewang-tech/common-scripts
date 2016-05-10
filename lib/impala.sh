impala_host() {
  yarn node -list -states RUNNING | tail -1 | cut -f 1 | cut -f 1 -d :
}

impala() {
  if [ -z $IMPALA_HOST ]; then
    impala-shell "$@"
  else
    impala-shell -i $IMPALA_HOST "$@"
  fi
}

impala_refresh() {
  local table=$1
  info "Computing statistics for Impala table $table ..."
  impala -q "refresh $table; compute stats $table;"
}

# print out the table stats
impala_table_stats() {
  local table=$1
  impala -B -q "show table stats $table"
}

# Get the output of table's stats without the roll up line 
impala_partitions() {
  local table=$1

  # fix the bug 'failed when no partition exists'
  ( impala_table_stats $table | grep -v 'Total' ) || return
}

escape() {
  sed -e 's/\//\\\//g'
}

hiveconf_pattern() {
  if [[ "$1" =~ ^(.+)=(.+)$ ]]
  then
    local value=$(echo ${BASH_REMATCH[2]} | escape)
    echo "s/\${hiveconf:${BASH_REMATCH[1]}}/$value/g"
  else
    exit -1
  fi
}

subst_param() {
  local pattern=""
  for p in $@
  do
    pattern="$pattern;$(hiveconf_pattern $p)"
  done
  sed -e "$pattern"
}

impala_p() {
  local file=$1
  shift
  local params=$@

  if [ ! -e $file ]; then
    error "Cannot find $file ..."
    exit -1
  fi

  local query=$(cat $file | subst_param "$params")
  impala -q "$query"
}
