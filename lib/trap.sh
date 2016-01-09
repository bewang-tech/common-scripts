trap on_interrupt INT 
# need to set errtrace to trap a exit in a function
# set -o errtrace
trap on_error ERR
trap on_terminate TERM

on_interrupt() {
  warn "Terminated by user."
  exit $?
}

stack_trace() {
  local n=${#FUNCNAME[@]}
  for ((i=1; i<n; i++)) {
    echo -e "    in ${FUNCNAME[i]} of ${BASH_SOURCE[i]}:${BASH_LINENO[$i-1]}" 
  }
}

on_error() {
  local error_code="$?"
  test $error_code == 0 && return;

  error "Failed with error $error_code"

  stack_trace

  exit $error_code
}

abort() {
  kill -SIGTERM $$
}

on_terminate() {
  error "Aborted!"
  stack_trace
  exit -1
}
