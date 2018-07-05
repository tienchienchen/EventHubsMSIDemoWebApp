#!/bin/ksh

if [ $# -lt 4 ]
then
	    echo "Usage: $0 oracle_sid schema_name proc_name dev|qa|preprod|prod" >&2
	        exit 1
	fi

	. ~oracle/.profile >/dev/null 2>&1

	DB=$1
	USER=$2
	PROC=$3
	ENVTYPE=$4
	GREP=$5

	PASS=`grep "|$DB|" ~oracle/db_password.txt | grep "^$ENVTYPE|" | grep "|$USER|" | head -1 | awk -F'|' '{print $NF}'`

	echo "`date` `basename $0` Started For $DB $USER.$PROC..."

	LOCKFILE="/tmp/${DB}_$USER.$PROC.LOCK"
	LOGFILE="/tmp/${DB}_$USER.$PROC.out"

	# set the umask to create a read only file.  If another process kicks off and
	# tries to create it it will fail and know it already exists

	umask 0222

	cat /dev/null 2>/dev/null >$LOCKFILE

	if [ $? -ne 0 ]
	then
		    echo "`date` $LOCKFILE Found..." >&2
		        exit 1
		fi

		umask 0022

		sqlplus -s /nolog >$LOGFILE 2>&1 <<!
		    CONNECT $USER/$PASS@$DB;
		        set serveroutput on;
			    EXEC $PROC
			    !

			    if [ $? -ne 0 -o `grep ERROR $LOGFILE | wc -l` -gt 0 ]
			    then
				        cat $LOGFILE >&2
					    echo "`date` `basename $0` FAILED..." >&2
					        rm -f $LOGFILE $LOCKFILE 2>/dev/null
						    exit 1
					    fi

					    if [ ! -z "$GREP" ]
					    then
						        egrep $GREP $LOGFILE
						fi

						echo "`date` `basename $0` SUCCESS..."

						rm -f $LOCKFILE $LOGFILE 2>/dev/null

						exit 0
