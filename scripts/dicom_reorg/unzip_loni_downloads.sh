#!/bin/bash
#SBATCH --mem=8G
#SBATCH --time=1:30:00
#SBATCH --array=0-9

# TESTING='UNCOMMENT_IF_TESTING'

echo "========== START TIME =========="
echo `date`

if [ ! -z $SLURM_JOB_ID ]
then
    echo "========== JOB SETTINGS =========="
    TIMELIMIT="$(squeue -j ${SLURM_JOB_ID} -h --Format TimeLimit | xargs)"
    echo "SLURM_JOB_ID:        $SLURM_JOB_ID"
    echo "SLURM_MEM_PER_NODE:  $SLURM_MEM_PER_NODE"
    echo "TIMELIMIT:           $TIMELIMIT"
    echo "SLURM_ARRAY_TASK_ID: $SLURM_ARRAY_TASK_ID"
    echo "WORKING DIRECTORY:   `pwd`"
fi

if [ ! -z $TESTING ]
then
    SLURM_ARRAY_TASK_ID=0
    echo "This is a test, setting SLURM_ARRAY_TASK_ID to $SLURM_ARRAY_TASK_ID"
fi

echo "========== SETTINGS =========="

if [ $# -ne 2 ]
then
    echo "Usage: $0 FPATH_ZIP DPATH_DEST"
    exit 1
fi

FPATH_ZIP="$1"
DPATH_DEST="$2"

echo "FPATH_ZIP:             $FPATH_ZIP"
echo "DPATH_DEST:            $DPATH_DEST"

# append suffix to filepath
if [[ ! -z $SLURM_ARRAY_TASK_ID ]]
then
    SUFFIX=$SLURM_ARRAY_TASK_ID
    FPATH_ZIP="${FPATH_ZIP%.*}-$SUFFIX.${FPATH_ZIP##*.}"

    echo "SUFFIX:                $SUFFIX"
    echo "FPATH_ZIP with suffix: $FPATH_ZIP"
fi

echo "========== MAIN =========="

# input validation
if [ ! -f $FPATH_ZIP ]
then
    echo "[ERROR] File not found: $FPATH_ZIP"
    exit 2
fi

# create destination directory if needed
if [ ! -d $DPATH_DEST ]
then
    echo "[INFO] Creating directory $DPATH_DEST since it does not exist"
    if [ -z $TESTING ]
    then
        mkdir -p $DPATH_DEST
    fi
fi

# -q: quiet
# -o: overwrite
# -d: output directory
UNZIP_COMMAND="unzip -qo $FPATH_ZIP -d $DPATH_DEST " # add -o to overwrite
echo "[RUN] $UNZIP_COMMAND"

if [ -z $TESTING ]
then
    eval "$UNZIP_COMMAND"
fi

echo "========== END TIME =========="
echo `date`
