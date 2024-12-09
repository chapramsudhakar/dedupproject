IP=50
PERS=20

#for TR in vidimg 
for TR in web mail linux bookppt vidimg
do
	for SEG in 16 
	do
		for cache in lru
		do
			if [ $TR = "mail" ] 
			then
				trdef="#define	MAIL	1"
			elif [ $TR = "web" ] 
			then
				trdef="#define	WEB	1"
			elif [ $TR = "home" ] 
			then 
				trdef="#define	HOME	1"
			elif [ $TR = "linux" ]
			then 
				trdef="#define	LINUX	1"
			elif [ $TR = "bookppt" ]
			then 
				trdef="#define	BOOKPPT	1"
			elif [ $TR = "vidimg" ]
			then 
				trdef="#define	VIDIMG	1"
			fi

			if [ $cache = "darc" ] 
			then
				cachedef="#define	DARCP	1"
				arc="#define ARC	0"
			else
				if [ $cache = "arc" ] 
				then
					cachedef="#define	DARCP	1"
					arc="#define ARC	1"
				else
					cachedef=""
					arc="#define ARC	0"
				fi
			fi
			for sn in 1 2 4 8 16 32 64
			do
				echo creating dedupconfig.h file

							
				cat << EOF > dedupconfig.h
#ifndef	__DEDUPCONFIG_H__
#define	__DEDUPCONFIG_H__

#define	WORK2	1
$trdef
$cachedef
$arc
#define	CPERCENT	$PERS
#define	IPPERCENT	$IP
#define	BUCKET3	1
#define MAX_SEG_SIZE	16
#define	MAX_DEDUP_SEG_SIZE	$SEG
#define	MAX_ROUTING_SIZE	256
#define	SNCOUNT		$sn
#endif
EOF

				#cat dedupconfig.h
				#echo make ddedup-app 
				#make ddedup-app 
				#echo mv ddedup-app ddedup-app-$TR-$sn
				#mv ddedup-app ddedup-app-$TR-$sn
				echo make ddedup-extbin 
				make ddedup-extbin 
				echo mv ddedup-extbin ddedup-extbin-$TR-$sn
				mv ddedup-extbin ddedup-extbin-$TR-$sn
				sleep 1
			done
		done
	done
done

