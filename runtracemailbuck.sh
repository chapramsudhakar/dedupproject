DIR=$1
TR=$2
CACHE=$3
DEDUP=$4

echo cd $1
cd $1
for PERS in 2 
do
	echo ./mkdata.sh
	./mkdata.sh
	echo time ../$DEDUP-$CACHE-$TR-$PERS 
	time ../$DEDUP-$CACHE-$TR-$PERS > $TR-$CACHE-$DEDUP-$PERS-percent
done

