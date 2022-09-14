#!/bin/bash
# for i in {1..50}; do go test || break; done > test_results

for t in Test1 # add all the test names here
do
  echo $t
  count=0
  n=100
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.txt
    result=$(grep -E '^PASS$' log-${t}-${i}.txt| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.txt
    fi
  done
  echo "$count/$n"
done