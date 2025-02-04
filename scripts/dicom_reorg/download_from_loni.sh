#!/bin/bash

if [ $# -lt 3 ]
then
    echo -e "Usage: $0 <FPATH_URLS> <HOST> <DPATH_REMOTE>\n"
    echo "<FPATH_URLS>:     file containing all the download links (available from LONI after initiating the download)"
    echo "<HOST>:           remote host machine address"
    echo "<DPATH_REMOTE>:   destination folder on remote host machine"

    echo -e "\nExample usage:"
    echo "./download_from_loni.sh session_BL.csv narval <DATASET_ROOT>/downloads/ses-BL"
    echo "(where ~/ppmi is the nipoppy dataset directory)"

    echo -e "\nNotes:"
    echo "- This script exists because LONI only allows downloads that are made from"
    echo "  the same computer that initiated the download on LONI. Since we cannot"
    echo "  easily initialize the download from a remote server (requires GUI/Internet),"
    echo "  the files need to be downloaded locally and then transfered to the remote server."
    echo "  This script saves local disk space by piping the download directly to the remote server."
    echo "- Sample unzipping command: unzip -q session_BL.zip -d <DATASET_ROOT>/scratch/raw_dicom/ses-BL/ &"
    exit 1
fi

FPATH_URLS=$1       # file containing all the download links (from LONI) (can include metadata link)
HOST=$2             # remote host machine address
DPATH_REMOTE=$3     # destination directory on remote host machine

SUFFIX=".zip"

# create new destination directory if needed
ssh $HOST "mkdir -p $DPATH_REMOTE"

I_FILE=1

for URL in `cat $FPATH_URLS`
do
    # grab the filename from the URL
    FILENAME=${URL##*/}
    FILENAME=${FILENAME##*=} # needed for metadata file

    # add an index number because sometimes there are files that are named the same...
    FILENAME="`basename $FILENAME $SUFFIX`-$I_FILE$SUFFIX"

    # check if file already exists
    FPATH_REMOTE=$DPATH_REMOTE/$FILENAME
    FILE_EXISTS=`ssh $HOST "ls $FPATH_REMOTE && echo $FPATH_REMOTE" 2>/dev/null`

    if [[ -z $FILE_EXISTS ]]
    then

        echo "Downloading $FPATH_REMOTE"

        # call wget from local machine, dump to stdout
        # and pipe to remote host using ssh
        COMMAND="(
            wget -O - '$URL' \
            | ssh $HOST 'cat > $FPATH_REMOTE'
        ) &"

    elif [[ $FILE_EXISTS == $FPATH_REMOTE ]]
    then

        # don't overwrite file
        echo "$FPATH_REMOTE already exists on $HOST. Not downloading"

    else
        echo "Error: $FILE_EXISTS"
        exit 1

    fi

    echo $COMMAND
    eval $COMMAND

    I_FILE=$((I_FILE+1))
done
