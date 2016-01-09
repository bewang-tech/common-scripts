format_date() {
  local format=${DATE_FORMAT:-%Y-%m-%d}
  date --date "$1" +${format}
}

dates_between () {
  local start_date=$(format_date "$1")
  local end_date=$(format_date "$2 1 day")

  local curr_date=$start_date
  until [ "$curr_date" == "$end_date" ]
  do
    echo $curr_date
    curr_date=$(format_date "$curr_date 1 day")
  done
}

