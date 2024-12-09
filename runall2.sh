
cd data40

for TR in vidimg 
do
	for SEG in 16 32 64 128 256
	do
		./mkdata.sh
		echo Running dedup-15-app-$TR-$SEG ... `date`
		../dedup-15-app-$TR-$SEG > app-$TR-$SEG 2>&1
	done
done

TR=linux
SEG=16
./mkdata.sh
echo Running dedup-15-bucket-$TR-$SEG ... `date`
../dedup-15-bucket-$TR-$SEG > bucket-$TR-$SEG 2>&1

echo all programs are completed ...


