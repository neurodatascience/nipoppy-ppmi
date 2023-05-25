#!/bin/bash

if [ $# -lt 3 ]
then
    echo -e "Usage: $0 <FPATH_URLS> <HOST> <DPATH_REMOTE>\n"
    echo "<FPATH_URLS>:     file containing all the download links (available from LONI after initiating the download)"
    echo "<HOST>:           remote host machine address"
    echo "<DPATH_REMOTE>:   destination folder on remote host machine"

    echo -e "\nExample usage:"
    echo "./download_from_loni.sh 20230310-1.csv bic /data/pd/ppmi/downloads/20230310/ses-1"

    echo -e "\nNotes:"
    echo "- This script needs to be run on the same computer that initiated the download on LONI"
    echo "- Sample unzipping command: unzip -q 20230310-1.zip -d /data/pd/ppmi/scratch/raw_dicom/ses-1/ &"
    exit 1
fi

FPATH_URLS=$1       # file containing all the download links (from LONI) (can include metadata link)
HOST=$2             # remote host machine address
DPATH_REMOTE=$3     # destination directory on remote host machine

# create new destination directory if needed
ssh $HOST "mkdir -p $DPATH_REMOTE"

for URL in `cat $FPATH_URLS`
do
    # grab the filename from the URL
    FILENAME=${URL##*/}
    FILENAME=${FILENAME##*=} # needed for metadata file

    # check if file already exists
    FPATH_REMOTE=$DPATH_REMOTE/$FILENAME
    FILE_EXISTS=`ssh bic "[[ -f $FPATH_REMOTE ]] && echo $FPATH_REMOTE"`

    if [[ -z $FILE_EXISTS ]]
    then

        echo "Downloading $FPATH_REMOTE"

        # call wget from local machine, dump to stdout
        # and pipe to remote host using ssh
        COMMAND="(
            wget -O - '$URL' \
            | ssh $HOST 'cat > $FPATH_REMOTE'
        ) &"

    else

        # don't overwrite file
        echo "$FPATH_REMOTE already exists on $HOST. Not downloading"

    fi

    echo $COMMAND
    eval $COMMAND
done
