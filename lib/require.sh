declare -A MODULE_MAP

load_module() {
  local module=$1
  local path=$2

  if [ -f $path ]; then
    MODULE_MAP[$module]=$path
    source $path
  fi
}

require() {
  local module=$1

  if [ ! ${MODULE_MAP[$module]+_} ]; then
    for p in $LIB_DIR $MODULE_DIR; do
      load_module $module $p/$module.sh
      if [ ${MODULE_MAP[$module]+_} ]; then
	break
      fi
    done

    if [ ! ${MODULE_MAP[$module]+_} ]; then
      exit -1
    fi
  fi
}
