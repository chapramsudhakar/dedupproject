
# data1 - hds-lru-mail
# data2 - hds-lru-web
# data3 - hds-lru-home
# data4 - hds-darc-mail
# data5 - hds-darc-web
# data6 - hds-darc-home

# data7 - full-lru-mail
# data8 - full-lru-web
# data9 - full-lru-home
# data10 - full-darc-mail
# data11 - full-darc-web
# data12 - full-darc-home

# data13 - native-mail
# data14 - native-web
# data15 - native-home

#./runtrace.sh data2 web lru dedup-15-bucket >> LOG 2>&1 
#./runtrace.sh data5 web darc dedup-15-bucket >> LOG 2>&1 

#echo waiting for finishing simulation with web traces
#wait
#echo waiting for simulation with web traces finished 

#./runtracemail.sh data1 mail lru dedup-15-bucket >> LOG 2>&1 
#./runtracebuck.sh data1 mail lru dedup-15-bucket >> LOG 2>&1 
#wait
#./runtracefull.sh data7 mail lru dedup-15-full >> LOG 2>&1 

#echo waiting for finishing simulation with lru mail traces
#wait
#echo waiting for simulation with lru mail traces finished 

#./runtracemail.sh data4 mail darc dedup-15-bucket >> LOG 2>&1 
#./runtracemailbuck.sh data1 mail lru dedup-15-bucket >> LOG 2>&1 
#./runtracemailbuck.sh data4 mail darc dedup-15-bucket >> LOG 2>&1 
#wait

#./runtracefull.sh data10 mail darc dedup-15-full >> LOG 2>&1 

#echo waiting for finishing simulation with darc mail traces
#wait
#echo waiting for simulation with darc mail traces finished 

#./runtrace.sh data3 home lru dedup-15-bucket >> LOG 2>&1 
#./runtracefull.sh data9 home lru dedup-15-full >> LOG 2>&1 

#echo waiting for finishing simulation with lru home traces
#wait
#echo waiting for simulation with lru home traces finished 

#./runtracebuck.sh data6 home darc dedup-15-bucket >> LOG 2>&1 
#wait
#wait

#echo waiting for finishing simulation with darc home traces
#wait
#echo waiting for simulation with darc home traces finished 
echo all programs are completed ...


