create_hive_table() {
  local ddl=$1
  local name=$2
  local loc=$3

  hive -f $ddl --hiveconf table_name=$name --hiveconf table_loc=$loc
}
