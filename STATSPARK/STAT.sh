#!/bin/bash

. ~warehouse/.bash_profile > /dev/null 2>&1
echo "`date` `basename $0` $1 Started..."
MYENV=$1

LOGFILE="/tmp/$MYENV`basename $0`.out"
date > $LOGFILE 

cd /home/warehouse/scripts/;./tagsList.sh $MYENV > "/home/warehouse/logs/"$MYENV"tagsList.log" 2 >&1
echo "END tagsList.sh"
cat "/home/warehouse/logs/"$MYENV"tagsList.log" >> $LOGFILE
cd /home/warehouse/scripts/;./keywordList.sh $MYENV > "/home/warehouse/logs/"$MYENV"keywordList.log" 2 >&1
echo "END keywordList.sh"
cat "/home/warehouse/logs/"$MYENV"keywordList.log" >> $LOGFILE
cd /home/warehouse/scripts;./keywordTSV.sh $MYENV > "/home/warehouse/logs/"$MYENV"keywordTSV.log" 2>&1
echo "END keywordTSV.sh"
cat "/home/warehouse/logs/"$MYENV"keywordTSV.log" >> $LOGFILE
cd /home/warehouse/scripts/;./bulkExport.sh $MYENV > "/home/warehouse/logs/"$MYENV"bulkExport.log" 2 >&1
echo "END bulkExport.sh"
cat "/home/warehouse/logs/"$MYENV"bulkExport.log" >> $LOGFILE

curl -n \
	-X POST -H 'Content-Type: application/json' \
	-d '{  "job_id": 8  }' https://eastus2.azuredatabricks.net/api/2.0/jobs/run-now

echo "END call databricks job_id 8"

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

