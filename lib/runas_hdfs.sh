if [ $USER != 'hdfs' ]; then
  error "This script must be run as 'hdfs'!"
  exit -1
fi
