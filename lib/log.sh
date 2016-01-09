#!/bin/bash

timestamp() {
  date +'%Y-%m-%d %H:%M:%S'
}

log() {
  local style=$1
  local log_type=$2
  shift 2

  printf "\e[${style}m[${log_type}] %s - %s\e[m\n" "$(timestamp)" "$@" 1>&2
}

info() {
  log "1;34" INFO "$@"
}

success() {
  log "1;32" SUCCESS "$@"
}

warn() {
  log "1;33" WARN "$@"
}

error() {
  log "1;31" ERROR "$@"
}
