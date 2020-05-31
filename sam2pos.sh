#!/usr/bin/env bash
set -o errexit
set -o nounset
set -e 
set -o pipefail
set -v

CURRENT_REFERENCE_FULL=$1
SAM_FOLDER=$2
SENSIBILITY=4
SAM_TO_POS_PY=./components/sam_to_pos/scripts/sam_to_positions.py

BASE=$(basename -- "$CURRENT_REFERENCE_FULL")
SAM=${SAM_FOLDER}/${BASE}.sam
#Filter mapped reads
echo " grep -h ${BASE} > ${SAM}"
grep -h ${BASE} $SAM_FOLDER/mapped*.sam > ${SAM}

POSITIONS_FILE=$2/${BASE}.pos
echo "sam_to_positions.py $SAM_TO_POS_PY $SAM $CURRENT_REFERENCE_FULL $SENSIBILITY"
python $SAM_TO_POS_PY $SAM $CURRENT_REFERENCE_FULL $SENSIBILITY > $POSITIONS_FILE
