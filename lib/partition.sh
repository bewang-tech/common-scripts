# Generate Hive table partition specification:
# 
# A Hive table may have multiple partition columns with different
# types, and string type columns need quotes on partition column value.
#
# Because the caller knows about the partition columns for the table,
# it is really convenient when the spec in printf format like this:
#   spec="device='%s',log_date='%s',hour=%d"
# which includes all information in one string.
# 
# And the partition values are in multiple lines and separated by blanks.
#
#   vals="
#     android 2014-07-01 0
#     android 2014-07-01 1
#     iphone  2014-07-02 5
#   "
#
# which can be easily gotten from impala query "show table stats $table" 
# 
# The function will output the hive partition spec, one line for a partition
#
#   device='android',log_date='2014-07-01',hour=0
#   device='android',log_date='2014-07-01',hour=1
#   device='iphone',log_date='2014-07-02',hour=5
#

# Given a partition spec format, generate an list of awk fields for all
# partition fields: ,$1,$2,$3 ... which will be used by the functions
# like partition_spec and partition_loc.

partition_spec() {
  local spec_format=$1
  local field_list=$3

  local prog=$(printf 'NF { printf "%s\\n", %s  }' "$spec_format" "$field_list")

  awk "$prog"
}

partition_loc() {
  local loc_format=$2
  local field_list=$3

  local prog=$(printf 'NF { printf "%s\\n", %s }' "$loc_format" "$field_list")

  awk "$prog"
}

partition_spec_loc() {
  local spec_format=$1
  local loc_format=$2
  local field_list=$3

  local prog=$(printf 'NF { printf "%s %s\\n", %s, %s }' "$spec_format" "$loc_format" "$field_list" "$field_list")

  awk "$prog"
}

partition_columns() {
  sed 's/\([^=]*\)=\([^,]*\)/\1/g' | tr "," "\n"
}

filter_partition() {
  local spec_format=$1
  local filter=$2

  if [ -z "$filter" ]; then
    awk '{ print $0; }'
  else
    local named_vars=$(echo "$spec_format" | partition_columns | awk '{ pos+=1; printf("%s=$%d;", $1, pos);}')
    local filter_func=$(printf '{ %s if (%s) print $0; }' "$named_vars" "$filter")
  
    awk "$filter_func"
  fi
}

# transform a pair of partition_spec and loc to an add partition statement for the table
q_add_partition() {
  local table=$1
  awk 'NF { printf("alter table '$table' add if not exists partition (%s) location '\''%s'\'';\n", $1, $2) }'
}

# transform a partition_spec to a drop partition statement for the table
q_drop_partition() {
  local table=$1
  awk 'NF { printf("alter table '$table' drop if exists partition (%s);\n", $0) }'
}

# cleanup_partition [-d] <table> <filter>
cleanup_partition() {
  while getopts "dp" opt; do 
    case "$opt" in
      d) rm_data=1;;
      p) drop_part=1;;
    esac
  done

  shift $(($OPTIND-1))

  local table=${1^^}
  local filter=$2

  local info_name="${table}[@]"
  local info=( ${!info_name} )

  local vals=$(impala_partitions $table | filter_partition "${info[0]}" "$filter")

  if [ -n "$drop_part" ]; then
    local query=$(echo "$vals" | partition_spec "${info[@]}" | q_drop_partition $table)
    if [ -n "$query" ]; then
      impala -q "$query"
    fi
  fi

  if [ -n "$rm_data" ]; then
    # TODO: if the partition is not present in the table, the directory won't be
    #   removed. We need to scan the base dir and get all files, then find out
    #   the dirs being removed.
    local locs=$(echo "$vals" | partition_loc "${info[@]}")
    if [ -n "$locs" ]; then
      ensure_not_exist $locs
    fi
  fi
}

partition_num_rows() {
  local table=$1
  local part_spec=$2

  # impala-shell outputs a control sequence ESC[?1034h before the result.
  # Reset TERM can remove the control sequence.
  TERM= impala -B -q "select count(1) from $table where $part_spec"
}

# count rows of the specified partition of the table
# update numRows of the partition
q_update_partition_stats() {
  local table=$1
  local part_spec=$2

  echo "compute incremental stats $table partition($part_spec);"
}
