
cd data40
#for TR in vidimg 
./mkdata.sh 64
echo Running ddedup-app-web-64 ... `date`
../ddedup-app-web-64 > app-web-64 2>&1

./mkdata.sh 64
echo Running ddedup-linux-web-64 ... `date`
../ddedup-app-linux-64 > app-linux-64 2>&1

for TR in bookppt vidimg 
do
	for sn in 1 2 4 8 16 32 64
	do
		./mkdata.sh $sn
		echo Running ddedup-app-$TR-$sn ... `date`
		../ddedup-app-$TR-$sn > app-$TR-$sn 2>&1
		#echo Running ddedup-extbin-$TR-$sn ... `date`
		#../ddedup-extbin-$TR-$sn > extbin-$TR-$sn 2>&1
	done
done

echo all programs are completed ...


