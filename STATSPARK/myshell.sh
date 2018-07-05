ps -ef | awk -v PID=$$ '{ if ( $2 == PID ) { gsub ("-","")  ; print $NF}}'
