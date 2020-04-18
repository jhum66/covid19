#!/bin/bash
NOW=$(date +"%s")
SOON=$(date -j 1930 +%s)
INTERVAL=$(($SOON-$NOW))

#echo $NOW
#echo $SOON
echo Sleeping for $INTERVAL seconds

sleep $INTERVAL


while true
do
    cd ..
    cd COVID-19
    git pull
    cd ..
    cd covid19
    python3 import_data.py
    python3 import_us_data.py

    NOW=$(date +"%s")
    SOON=$(date -j 1930 +%s)
    INTERVAL=$(($SOON-$NOW+86400))

    echo Sleeping for $INTERVAL seconds

    sleep $INTERVAL
    #sleep 86400
done
