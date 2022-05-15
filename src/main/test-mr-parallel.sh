# run the test in a fresh sub-directory.
RACE=-race
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go clean)
(cd .. && go clean)
# (cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
# (cd .. && go build $RACE mrsequential.go) || exit 1

#########################################################
# echo '***' Starting map parallelism test.

# rm -f mr-*

# $TIMEOUT ../mrcoordinator ../pg*txt &
# sleep 1

# $TIMEOUT ../mrworker ../../mrapps/mtiming.so &
# $TIMEOUT ../mrworker ../../mrapps/mtiming.so

# NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
# if [ "$NT" != "2" ]
# then
#   echo '---' saw "$NT" workers rather than 2
#   echo '---' map parallelism test: FAIL
#   failed_any=1
# fi

# if cat mr-out* | grep '^parallel.* 2' > /dev/null
# then
#   echo '---' map parallelism test: PASS
# else
#   echo '---' map workers did not run in parallel
#   echo '---' map parallelism test: FAIL
#   failed_any=1
# fi

# wait

#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

$TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

$TIMEOUT ../mrworker ../../mrapps/rtiming.so &
$TIMEOUT ../mrworker ../../mrapps/rtiming.so &
$TIMEOUT ../mrworker ../../mrapps/rtiming.so &
$TIMEOUT ../mrworker ../../mrapps/rtiming.so &
$TIMEOUT ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces: $NT
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait
