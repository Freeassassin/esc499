s/\r$//
s/interval '([0-9]+)' day \(3\)/interval \1 day/g
s/interval '([0-9]+)' month/interval \1 month/g
s/interval '([0-9]+)' year/interval \1 year/g
s/date '([^']+)'/date '\1'/g
/^[[:space:]]*limit -1;[[:space:]]*$/d
/^\s*$/N
/^\n$/D
