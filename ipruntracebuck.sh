DIR=$1
TR=$2
CACHE=$3
DEDUP=$4
PERS=2

echo cd $1
cd $1
for IP in 10 20 30 40 50 60 70 80 90 92 94 96 98
do
	echo ./mkdata.sh
	./mkdata.sh
	echo time ../$DEDUP--$IP-$PERS 
	time ../$DEDUP--$IP-$PERS > mail-$IP-$CACHE-$DEDUP-$PERS-percent
done

