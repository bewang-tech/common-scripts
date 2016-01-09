# Ensure the directory exists. If not, create the directory.
# If the path exists and is a file, return non-zero
ensure_exist_dir() {
  local dir=$1

  info "Ensuring dir $dir exist ..."
  if ! $(hdfs dfs -test -d "$dir");  then
    # not exists or is a file
    if ! $(hdfs dfs -test -e "$dir"); then
      # not exists
      info "Creating directory $dir ..."
      hdfs dfs -mkdir -p $dir
    else
      exit 1
    fi  
  fi
}

# Ensure the list of paths (files or directories) not exist
# Remove it if a path exists.
# The function accepts option -skipTrash and 
ensure_not_exist() {
  hdfs dfs -rm -r -f "$@"
}

# Ensure the specified diretories exist and empty.
# If a path is an existing file or directory, it will be 
# removed first. 
ensure_empty_dir() {
  hdfs dfs -rm -r -f "$@"
  hdfs dfs -mkdir -p "$@"
}
