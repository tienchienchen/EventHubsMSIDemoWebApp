#!/bin/bash
# wget the first 50000 keywords (there are ~47k of them)
# Jonathon Williams 2017-08-04 Initial
# Richard Chen 2018-06-26
. ~warehouse/.bash_profile > /dev/null 2>&1
echo "`date` `basename $0` $1 Started..."
MYENV=$1


LOGFILE="/tmp/$MYENV`basename $0`.out"
LOCKFILE="/tmp/$MYENV`basename $0`.LOCK"

echo $LOGFILE  > $LOGFILE 
#:<<'EOF'
rm -Rf   /home/warehouse/files/keywordList/$MYENV/*
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=0' -O /home/warehouse/files/keywordList/$MYENV/keywordList0.json >> $LOGFILE 2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=5000' -O /home/warehouse/files/keywordList/$MYENV/keywordList5000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=10000' -O /home/warehouse/files/keywordList/$MYENV/keywordList10000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=15000' -O /home/warehouse/files/keywordList/$MYENV/keywordList15000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=20000' -O /home/warehouse/files/keywordList/$MYENV/keywordList20000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=25000' -O /home/warehouse/files/keywordList/$MYENV/keywordList25000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=30000' -O /home/warehouse/files/keywordList/$MYENV/keywordList30000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=35000' -O /home/warehouse/files/keywordList/$MYENV/keywordList35000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=40000' -O /home/warehouse/files/keywordList/$MYENV/keywordList40000.json >> $LOGFILE  2>&1
wget 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/keywords/list?site_id=539&format=json&results=5000&start=45000' -O /home/warehouse/files/keywordList/$MYENV/keywordList45000.json >> $LOGFILE  2>&1
#EOF

cd /home/warehouse/files/keywordList/$MYENV/
todate=`date +%Y%m%d`
number=`date -d "$todate -1 days" "+%Y%m%d"`
for f in *; 
do   
	#/home/warehouse/bin/az dls fs upload --overwrite --account rentpathdatalake --source-path $f --destination-path /clusters/rentpathdatalake/IN/API-STAT/Keywords/$number/$f; 
 /usr/bin/az dls fs upload --overwrite --account rentpathdatalake --source-path $f --destination-path /$MYENV/Input/STAT/Keywords/brand=aptg/yyyy=`date -d "$number " "+%Y"`/mm=`date -d "$number " "+%m"`/dd=`date -d "$number " "+%d"`/$f; >>$LOGFILE 2>&1

done

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

