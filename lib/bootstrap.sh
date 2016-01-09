# This module is used for including all common modules
# A script locating in any folder can use all common modules by
# setting MODULE_DIR

set -e
set -o errtrace

CONTAINER_DIR=$(dirname $MODULE_DIR)
BI_M2REPO=$CONTAINER_DIR/.m2/repository

MODULE_NAME=$(basename $MODULE_DIR)
#
# Common module directories
BIN_DIR=$MODULE_DIR/bin
CONF_DIR=$MODULE_DIR/conf
LIB_DIR=$MODULE_DIR/lib
DDL_DIR=$MODULE_DIR/ddl

source $LIB_DIR/common/require.sh

require common/log
require common/trap
