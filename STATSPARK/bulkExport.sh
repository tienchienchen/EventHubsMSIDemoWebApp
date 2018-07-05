#!/bin/bash

. ~warehouse/.bash_profile > /dev/null 2>&1
echo "`date` `basename $0` $1 Started..."

	MYENV=$1 

#	LOCKFILE="/tmp/$MYENV`basename $0`.LOCK"
#	umask 0222
#	cat /dev/null 2>/dev/null >$LOCKFILE
#        if [ $? -ne 0 ]
#	        then
#	           echo "`date` $LOCKFILE Found..." >&2
#	          exit 1
#         fi

#	 umask 0022

	cd /home/warehouse/files/serps/$MYENV
	rm -Rf /home/warehouse/files/serps/$MYENV/*

LOGFILE="/tmp/$MYENV`basename $0`.out"
echo $LOGFILE
todate=`date +%Y%m%d`>$LOGFILE 2>&1
#echo $todate
date=`date -d "$todate -3 days" "+%Y%m%d"`
#echo $date

for ((number=$((`date -d "$date 1 days" "+%Y%m%d"`));  number < todate; number=$((`date -d "$number 1 days" "+%Y%m%d"`))))
do
	echo $number
	date=`date -d "$number" +%Y-%m-%d`
	echo $date
	url=$(curl -s 'https://rentpath.getstat.com/api/v2/f248cd1e1f75d72c7134c03242a76e5957a7342f/bulk/list?format=json&results=1000' | ~/jq -r ".Response.Result[] | select(.JobType==\"serps\" and .Date==\"$date\") | .Url")
	echo $url

	wget "$url" -O "/home/warehouse/files/serps/$MYENV/serps$number.json.gz" >>$LOGFILE 2>&1
       #If wget file is small than 100 byte  retry	
       while [ `ls -l /home/warehouse/files/serps/$MYENV/serps$number.json.gz|awk '{ print $5}'` -le 100 ]
       do
        echo "wget $url ERROR"
	sleep 10
	wget "$url" -O "/home/warehouse/files/serps/$MYENV/serps$number.json.gz" >>$LOGFILE 2>&1
       done

	echo "/usr/bin/az dls fs upload --overwrite --account rentpathdatalake --source-path "/home/warehouse/files/serps/$MYENV/serps$number.json.gz" --destination-path /$MYENV/Input/STAT/SERPS/brand=aptg/yyyy=`date -d "$number " "+%Y"`/mm=`date -d "$number " "+%m"`/dd=`date -d "$number " "+%d" `/serps$number.json.gz; "
	/usr/bin/az dls fs upload --overwrite --account rentpathdatalake --source-path "/home/warehouse/files/serps/$MYENV/serps$number.json.gz" --destination-path /$MYENV/Input/STAT/SERPS/brand=aptg/yyyy=`date -d "$number " "+%Y"`/mm=`date -d "$number " "+%m"`/dd=`date -d "$number " "+%d" `/serps$number.json.gz; >>$LOGFILE 2>&1 

done

# cat $LOGFILE

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

#number=$((`date -d "$number -1 days" "+%Y%m%d"`))
#YYYY=$((`date -d "$number " "+%Y"`))
#MM=$((`date -d "$number " "+%m"`))
#date -d "$number " "+%m"
#DD=$((`date -d "$number" "+%d"`))
#echo $number
#echo $YYYY
#echo $MM
#echo $DD
#for f in *; 
#do   
#	echo ""
#	/usr/bin/az dls fs upload --account rentpathdatalake --source-path $f --destination-path /$MYENV/Input/STAT/SERPS/brand=aptg/yyyy=$YYYY/mm=`date -d "$number " "+%m"`/dd=`date -d "$number " "+%d" `/$f; 
	#echo "/usr/bin/az dls fs upload --account rentpathdatalake --source-path $f --destination-path /$MYENV/Input/STAT/SERPS/brand=aptg/yyyy=$YYYY/mm=`date -d "$number " "+%m"`/dd=`date -d "$number " "+%d" `/$f; "
	#echo `/usr/bin/az dls fs upload --account rentpathdatalake --source-path $f --destination-path /clusters/rentpathdatalake/IN/API-STAT/Bulk/$number/$f;`
#done
#gunzip *
#/usr/bin/az dls fs upload --account rentpathdatalake --source-path /home/warehouse/files/serps/$MYENV/serps$number.json --destination-path /$MYENV/input/STAT/$number/Bulk/JSON/serps$number.json

