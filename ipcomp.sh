PERS=2
cache=darc

for IP in 10 20 30 40 50 60 70 80 90 92 94 96 98 
do
	trdef="#define	MAIL	1"
	cachedef="#define	DARCP	1"
	arcdef="#define		ARC	0"

	echo creating dedupconfig.h file

						
	cat << EOF > dedupconfig.h
#ifndef	__DEDUPCONFIG_H__
#define	__DEDUPCONFIG_H__

#define	WORK2	1
$trdef
$cachedef
#define	ARC	0
#define	CPERCENT	$PERS
#define	IPPERCENT	$IP
#define	BUCKET3	1
#endif
EOF

	#cat dedupconfig.h
	echo make dedup-15-full dedup-15-bucket native-15-response
	make dedup-15-full dedup-15-bucket native-15-response
#	make dedup-15-bucket 
	echo mv dedup-15-full dedup-15-full-$cache-$IP-$PERS
	mv dedup-15-full dedup-15-full-$cache-$IP-$PERS
	echo mv dedup-15-bucket dedup-15-bucket-$cache-$IP-$PERS
	mv dedup-15-bucket dedup-15-bucket-$cache-$IP-$PERS
	echo mv native-15-response native-15-response-$cache-$IP-$PERS
	mv native-15-response native-15-response-$cache-$IP-$PERS
	sleep 1
done
