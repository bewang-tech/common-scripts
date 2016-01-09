# Module load_func
#
put_to_hdfs() {
  local target_file=$1
  hadoop fs $PUT_OPTS -put - $target_file
}

get_remote_file() {
  local host=$1
  local src_file=$2

  ssh $host "cat $src_file"
}

get_local_file() {
  local src_file=$1

  cat $src_file
}

get_zip_remote_file() {
  local host=$1
  local src_file=$2

  ssh $host "cat $src_file | gzip"
}

get_zip_local_file() {
  local src_file=$1

  cat $src_file | gzip
}

load_local_file() {
  local path=$1
  local dest_dir=$2

  local file=$(basename $path)
  
  if [[ "$file" =~ \.gz$ ]]
  then
    # already gzipped
    msg="copying $file ..."
    get_func=get_local_file
    dest_file=$dest_dir/$file
  else
    msg="gzipping and copying $file ..."
    get_func=get_zip_local_file
    dest_file=$dest_dir/$date/${file}.gz
  fi

  if $(hdfs dfs -test -e $dest_file); then
    info "skipping $file ..."
  else
    info "$msg"
    $get_func $host $path | put_to_hdfs $dest_file
  fi
}

# remote copy a .gz file to hdfs
load_remote_file() {
  local host=$1
  local path=$2
  local dest_dir=$3

  local file=$(basename $path)
  
  if [[ "$file" =~ \.gz$ ]]
  then
    # already gzipped
    msg="copying $file ..."
    get_func=get_remote_file
    dest_file=$dest_dir/$file
  else
    msg="gzipping and copying $file ..."
    get_func=get_zip_remote_file
    dest_file=$dest_dir/$date/${file}.gz
  fi

  if $(hdfs dfs -test -e $dest_file); then
    info "skipping $file ..."
  else
    info "$msg"
    $get_func $host $path | put_to_hdfs $dest_file
  fi
} 

ensure_partitions_exist() {
  local root=$1
  shift
  local dates=$@

  for d in $dates
  do
    ensure_exist_dir $root/$d
  done
}
