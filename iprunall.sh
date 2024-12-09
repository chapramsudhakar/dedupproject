# data16 - native-darc- mail-ip-2
# data17 - hds-darc-mail-ip-2
# data18 - full-darc-mail-ip-2

./ipruntrace.sh data16 mail darc  native-15-response >> LOG1 2>&1 &
./ipruntrace.sh data17 mail darc dedup-15-bucket >> LOG1 2>&1 &
./ipruntrace.sh data18 mail darc dedup-15-full >> LOG1 2>&1 &
#./ipruntracebuck.sh data17 mail darc dedup-15-bucket >> LOG1 2>&1 

echo all programs are completed ...


