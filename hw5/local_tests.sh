#!/bin/bash
# go test -list . | grep -v ok | tr '\n' ' '
# note: some test cases are deterministic; 1 run is enough to test correctness
num_test=50

echo "Part A (i): state_test"
cd pkg/pingpong
for t in TestHashAndEqual TestStateInherit TestNextStates TestPartition
do
  echo $t
  count=0
  n=$num_test
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.log
    result=$(grep -E '^PASS$' log-${t}-${i}.log| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.log
    fi
  done
  echo "$count/$n"
done
echo "done"
echo "Part A (ii): search_test"
for t in TestBasic TestBfsFind TestBfsFindAll1 TestBfsFindAll2 TestBfsFindAll3 TestRandomWalkFindAll TestRandomWalkFind
do
  echo $t
  count=0
  n=$num_test
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.log
    result=$(grep -E '^PASS$' log-${t}-${i}.log| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.log
    fi
  done
  echo "$count/$n"
done

echo "done"
echo "Part B: TestUnit"
cd ../paxos
for t in TestUnit
do
  echo $t
  count=0
  n=$num_test
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.log
    result=$(grep -E '^PASS$' log-${t}-${i}.log| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.log
    fi
  done
  echo "$count/$n"
done

echo "done"
echo "Part C (i): well-designed tests:"
for t in TestBasic TestBasic2 TestBfs1 TestBfs2 TestBfs3 TestInvariant TestPartition1 TestPartition2
do
  echo $t
  count=0
  n=$num_test
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.log
    result=$(grep -E '^PASS$' log-${t}-${i}.log| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.log
    fi
  done
  echo "$count/$n"
done

echo "Part C (ii): To-be-completed tests:"
for t in TestCase5Failures TestNotTerminate TestConcurrentProposer
do
  echo $t
  count=0
  n=$num_test
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.log
    result=$(grep -E '^PASS$' log-${t}-${i}.log| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.log
    fi
  done
  echo "$count/$n"
done