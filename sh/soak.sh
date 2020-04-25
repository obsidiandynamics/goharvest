#!/bin/bash

if [ "$SOAK_CMD" == "" ]; then
  echo "SOAK_CMD is not set"
  exit 1
fi
if [ "$SOAK_RUNS" == "" ]; then
  SOAK_RUNS=10
fi
if [ "$SOAK_INTERVAL" == "" ]; then
  SOAK_INTERVAL=0
fi

if [ "$SOAK_GITPULL" == "" ]; then
  SOAK_GITPULL=true
fi

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
GREY='\033[0;90m'
NC='\033[0m'

echo -e "${GREY}SOAK_CMD:      $SOAK_CMD${NC}"
echo -e "${GREY}SOAK_RUNS:     $SOAK_RUNS${NC}"
echo -e "${GREY}SOAK_INTERVAL: $SOAK_INTERVAL${NC}"
echo -e "${GREY}SOAK_GITPULL:  $SOAK_GITPULL${NC}"

cd $(dirname $0)/..

set -e
cycle=1
while [ true ]; do
  echo -e "${CYAN}=============================================================================================================${NC}"
  echo -e "${CYAN}Cycle ${cycle}${NC}"

  for run in $(seq 1 $SOAK_RUNS)
  do
    timestamp=$(date +%'F %H:%M:%S')
    echo -e "${GREEN}-------------------------------------------------------------------------------------------------------------${NC}"
    echo -e "${GREEN}${timestamp}: Starting run ${run}/${SOAK_RUNS}${NC}"
    echo -e "${GREEN}-------------------------------------------------------------------------------------------------------------${NC}"
    $SOAK_CMD

    sleep $SOAK_INTERVAL
  done

  if [ $SOAK_GITPULL == "true" ]; then
    git pull
  fi
  cycle=$(($cycle + 1))
done

