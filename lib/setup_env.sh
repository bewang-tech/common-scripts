require common/cdh
CDH_LIB_DIR=$(cdh_lib_dir)

# LOAD common scripts
require common/date
require common/hdfs
require common/java

require conf/bi/env
require conf/bi/env-site

# LOAD module's env.sh
require conf/env
