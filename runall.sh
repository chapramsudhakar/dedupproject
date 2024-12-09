
cd data40
#for TR in vidimg 
for TR in web mail linux bookppt vidimg 
do
	for sn in 1 2 4 8 16 32 64
	do
		./mkdata.sh $sn
		#echo Running ddedup-app-$TR-$sn ... `date`
		#../ddedup-app-$TR-$sn > app-$TR-$sn 2>&1
		echo Running ddedup-extbin-$TR-$sn ... `date`
		../ddedup-extbin-$TR-$sn > extbin-$TR-$sn 2>&1
	done
done

echo all programs are completed ...


