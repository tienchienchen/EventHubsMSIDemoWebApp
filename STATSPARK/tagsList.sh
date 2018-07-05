#!/bin/bash
# wget the tags from the stat api
# Jonathon Williams 2017-08-04 Initial
# Richard Chen  2018-06-26 

. ~warehouse/.bash_profile > /dev/null 2>&1
echo "`date` `basename $0` $1 Started..."

MYENV=$1 

LOGFILE="/tmp/$MYENV`basename $0`.out"
echo $LOGFILE

wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/tags/list?site_id=539&format=json&results=5000&start=0' -O /home/warehouse/files/tagsList/$MYENV/tagsList.json >$LOGFILE

todate=`date +%Y%m%d`
number=`date -d "$todate -1 days" "+%Y%m%d"`

/usr/bin/az dls fs upload --overwrite --account rentpathdatalake --source-path /home/warehouse/files/tagsList/$MYENV/tagsList.json --destination-path /$MYENV/Input/STAT/Tags/brand=aptg/yyyy=`date -d "$number " "+%Y"`/mm=`date -d "$number " "+%m"`/dd=`date -d "$number " "+%d" `/tagsList.json; >>$LOGFILE 2>&1 

if [ $? -ne 0 -o `grep ERROR $LOGFILE | wc -l` -gt 0 ]
then
    cat $LOGFILE >&2
    echo "`date` `basename $0` FAILED..." >&2
    rm -f $LOGFILE $LOCKFILE 2>/dev/null
    exit 1
fi


echo "`date` `basename $0` $1 SUCCESS..."
rm -f $LOCKFILE $LOGFILE 2>/dev/null
exit 0

