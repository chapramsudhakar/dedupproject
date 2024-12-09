all:	dedup-15-full dedup-15-bucket dedup-15-app native-15-response \
	ddedup-app ddedup-extbin \
	dedup-stat dedup-stat2 dedup-stat3 \
	content-stat content-stat2 convert-trace \
	readgen readgen2 csize hashtest \
	mkindex mkbucket mkbitmap mkpbaref mkfblist \
	examibtree examhbtree exambucket examhbucket examtrace examfilerecipe

CC=g++
# CFLAGS=-g -pg
# CFLAGS=-Wall -Wunused-but-set-variable 
# CFLAGS=-ggdb3 -g -O0 -mcmodel=large -Warray-bounds -Wall -Wunused-but-set-variable 
# CFLAGS=-O1 -mcmodel=large -Warray-bounds -Wall -Wunused-but-set-variable 
# CFLAGS=-O3 -mcmodel=large -Warray-bounds -Wall -Wunused-but-set-variable 
CFLAGS=-ggdb3 -g -O0 -mcmodel=large -Warray-bounds -Wall -Wunused-but-set-variable -lrt  
# CFLAGS=-O1 -mcmodel=large -Warray-bounds -Wall -Wunused-but-set-variable -lrt -fsanitize=address -fstack-protector-strong -D_FORTIFY_SOURCE=2 -lrt
# CFLAGS= -Wall -Wunused-but-set-variable

#deduplication main programs =====================

dedup-15-full:	util.o  iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o cachemem.o lru.o darc.o lrulist.o freelist.o hbtree.o ibtree.o fhbtree.o pbaref.o buckcomn.o bucket.o hbucket.o fblist.o metadata.o main-15-full.o
	$(CC) $(CFLAGS) -lm -o dedup-15-full util.o iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o cachemem.o lru.o darc.o lrulist.o freelist.o hbtree.o ibtree.o fhbtree.o pbaref.o buckcomn.o bucket.o hbucket.o fblist.o metadata.o main-15-full.o 

dedup-15-bucket:	util.o  iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o cachemem.o darc.o lrulist.o writebuf.o freelist.o ibtree.o buckcomn.o bucket.o hbucket.o metadata.o pbaref.o hbtree.o fhbtree.o fblist.o main-15-bucket.o
	$(CC) $(CFLAGS) -lm -o dedup-15-bucket util.o iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o darc.o lrulist.o cachemem.o writebuf.o freelist.o ibtree.o buckcomn.o bucket.o hbucket.o metadata.o pbaref.o hbtree.o fhbtree.o fblist.o main-15-bucket.o

dedup-15-app:	util.o  iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o cachemem.o darc.o lrulist.o writebuf.o freelist.o ibtree.o fhbtree.o hbtree.o buckcomn.o bucket.o hbucket.o pbaref.o fblist.o metadata.o md5.o main-15-app.o
	$(CC) $(CFLAGS) -lm -o dedup-15-app util.o iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o darc.o lrulist.o cachemem.o writebuf.o freelist.o ibtree.o fhbtree.o hbtree.o buckcomn.o bucket.o hbucket.o pbaref.o fblist.o metadata.o md5.o main-15-app.o

native-15-response:	util.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o darc.o lrulist.o cachemem.o main-15-native.o 
	$(CC) $(CFLAGS) -lm -o native-15-response util.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o darc.o lrulist.o cachemem.o main-15-native.o 

ddedup-app:	util.o  iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o cachemem.o darc.o lrulist.o writebuf.o freelist.o ibtree.o fhbtree.o hbtree.o buckcomn.o bucket.o hbucket.o pbaref.o fblist.o filerecipe.o metadata.o md5.o storagenode.o main-client.o
	$(CC) $(CFLAGS) -lm -o ddedup-app util.o iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o darc.o lrulist.o cachemem.o writebuf.o freelist.o ibtree.o fhbtree.o hbtree.o buckcomn.o bucket.o hbucket.o pbaref.o fblist.o filerecipe.o metadata.o md5.o storagenode.o main-client.o

ddedup-extbin:	util.o  iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o cachemem.o darc.o lrulist.o writebuf.o freelist.o ibtree.o fhbtree.o hbtree.o buckcomn.o bucket.o hbucket.o pbaref.o fblist.o filerecipe.o metadata.o md5.o storagenode.o main-extbin.o
	$(CC) $(CFLAGS) -lm -o ddedup-extbin util.o iotracereader4k.o iotracereaderhome.o iotracereader.o bitmap.o lru.o darc.o lrulist.o cachemem.o writebuf.o freelist.o ibtree.o fhbtree.o hbtree.o buckcomn.o bucket.o hbucket.o pbaref.o fblist.o filerecipe.o metadata.o md5.o storagenode.o main-extbin.o


#statistics of traces ============================

dedup-stat:	util.o dupset.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o cachemem.o hbtree.o ibtree.o freelist.o dedup-stat.o 
	$(CC) $(CFLAGS) -lm -o dedup-stat util.o dupset.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o cachemem.o hbtree.o ibtree.o freelist.o dedup-stat.o 

dedup-stat2:	util.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o cachemem.o hbtree.o ibtree.o freelist.o dedup-stat2.o 
	$(CC) $(CFLAGS) -lm -o dedup-stat2 util.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o cachemem.o hbtree.o ibtree.o freelist.o dedup-stat2.o 

dedup-stat3:	util.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o cachemem.o hbtree.o ibtree.o freelist.o dedup-stat3.o 
	$(CC) $(CFLAGS) -lm -o dedup-stat3 util.o iotracereader4k.o iotracereaderhome.o iotracereader.o lru.o cachemem.o hbtree.o ibtree.o freelist.o dedup-stat3.o 

content-stat:	content-stat.o iotracereader4k.o iotracereaderhome.o iotracereader.o
	$(CC) $(CFLAGS) -o content-stat content-stat.o iotracereader4k.o iotracereaderhome.o iotracereader.o	

content-stat2:	content-stat2.o iotracereader4k.o iotracereaderhome.o iotracereader.o
	$(CC) $(CFLAGS) -o content-stat2 content-stat2.o iotracereader4k.o iotracereaderhome.o iotracereader.o	


#utilities =============================

convert-trace:	convert-trace.o iotracereader4k.o iotracereaderhome.o iotracereader.o
	$(CC) $(CFLAGS) -o convert-trace convert-trace.o iotracereader4k.o iotracereaderhome.o iotracereader.o	

readgen:	readgen.o iotracereader4k.o iotracereaderhome.o iotracereader.o
	$(CC) $(CFLAGS) -o readgen readgen.o iotracereader4k.o iotracereaderhome.o iotracereader.o	

readgen2:	readgen2.o iotracereader4k.o iotracereaderhome.o iotracereader.o
	$(CC) $(CFLAGS) -o readgen2 readgen2.o iotracereader4k.o iotracereaderhome.o iotracereader.o	

csize:	csize.o 
	$(CC) $(CFLAGS) -o csize csize.o 

hashtest:	hashtest.cpp md5.o
	$(CC) $(CFLAGS) -o hashtest hashtest.cpp md5.o


#Making the datastructures ==========================

mkindex:	mkindex.cpp ibtree.h freelist.h darc.h cache.h lrulist.h lru.h  hbtree.h fhbtree.h filerecipe.h
	$(CC) $(CFLAGS) -o mkindex mkindex.cpp

mkfblist:	mkfblist.cpp fblist.h 
	$(CC) $(CFLAGS) -o mkfblist mkfblist.cpp

mkbucket:	mkbucket.cpp bucket.h dedupn.h dedupconfig.h buckcomn.h lru.h cache.h darc.h lrulist.h ibtree.h freelist.h writebuf.h cachesize.h iotracereader.h hbucket.h
	$(CC) $(CFLAGS) -o mkbucket mkbucket.cpp

mkbitmap:	mkbitmap.o bitmap.o
	$(CC) $(CFLAGS) -o mkbitmap mkbitmap.o bitmap.o

mkpbaref:	mkpbaref.o mkpbaref.o
	$(CC) $(CFLAGS) -o mkpbaref mkpbaref.o 


#Examining the datastructures =========================

examibtree:	examibtree.o ibtree.o lru.o cachemem.o freelist.o util.o dedupn.h
	$(CC) $(CFLAGS) -o examibtree examibtree.o ibtree.o lru.o cachemem.o freelist.o util.o

examfilerecipe:	examfilerecipe.o filerecipe.o storagenode.o metadata.o ibtree.o hbtree.o fblist.o fhbtree.o bucket.o hbucket.o buckcomn.o bitmap.o pbaref.o darc.o lru.o lrulist.o cachemem.o freelist.o util.o dedupn.h
	$(CC) $(CFLAGS) -o examfilerecipe examfilerecipe.o filerecipe.o storagenode.o metadata.o ibtree.o hbtree.o fblist.o fhbtree.o bucket.o hbucket.o buckcomn.o bitmap.o pbaref.o darc.o lru.o lrulist.o cachemem.o freelist.o util.o

examhbtree:	examhbtree.o hbtree.o lru.o cachemem.o freelist.o util.o dedupn.h
	$(CC) $(CFLAGS) -o examhbtree examhbtree.o hbtree.o lru.o cachemem.o freelist.o util.o

exambucket:	bucket.o buckcomn.o exambucket.cpp freelist.o util.o lru.o cachemem.o ibtree.o bitmap.o cachemem.h bucket.h dedupn.h dedupconfig.h buckcomn.h lru.h cache.h darc.h lrulist.h ibtree.h freelist.h writebuf.h cachesize.h iotracereader.h metadata.h hbtree.h fhbtree.h fblist.h hbucket.h pbaref.h
	$(CC) $(CFLAGS) -o exambucket exambucket.cpp bucket.o buckcomn.o freelist.o util.o lru.o cachemem.o ibtree.o bitmap.o
	
examhbucket:	hbucket.o buckcomn.o ibtree.o examhbucket.cpp freelist.o util.o lru.o cachemem.o bitmap.o dedupconfig.h dedupn.h hbucket.h freelist.h buckcomn.h lru.h cache.h darc.h lrulist.h cachesize.h cachemem.h ibtree.h writebuf.h
	$(CC) $(CFLAGS) -o examhbucket examhbucket.cpp hbucket.o buckcomn.o ibtree.o freelist.o util.o lru.o cachemem.o bitmap.o

examtrace:	examtrace.cpp iotracereader.h iotracereader4k.o iotracereaderhome.o iotracereader.o 
	$(CC) $(CFLAGS) -o examtrace examtrace.cpp iotracereader4k.o iotracereaderhome.o iotracereader.o 


#Compiling required c++ programs ======================

bitmap.o:	bitmap.cpp bitmap.h dedupconfig.h dedupn.h
	$(CC) $(CFLAGS) -c bitmap.cpp

buckcomn.o:	buckcomn.cpp buckcomn.h
	$(CC) $(CFLAGS) -c buckcomn.cpp

bucket.o: bucket.cpp bucket.h dedupn.h dedupconfig.h buckcomn.h lru.h cache.h darc.h lrulist.h ibtree.h freelist.h writebuf.h cachesize.h iotracereader.h cachemem.h metadata.h hbtree.h fhbtree.h fblist.h hbucket.h pbaref.h util.h storagenode.h bitmap.h
	$(CC) $(CFLAGS) -c bucket.cpp

cachemem.o:	cachemem.cpp cachemem.h cache.h writebuf.h dedupconfig.h dedupn.h cachesize.h
	$(CC) $(CFLAGS) -c cachemem.cpp

content-stat.o:	content-stat.cpp dedupn.h dedupconfig.h iotracereader.h dupset.h
	$(CC) $(CFLAGS) -c content-stat.cpp

content-stat2.o:	content-stat2.cpp dedupn.h dedupconfig.h iotracereader.h dupset.h 
	$(CC) $(CFLAGS) -c content-stat2.cpp

convert-trace.o:	convert-trace.cpp dedupn.h dedupconfig.h iotracereader.h
	$(CC) $(CFLAGS) -c convert-trace.cpp

csize.o:	csize.cpp dedupconfig.h dedupn.h bitmap.h buckcomn.h ibtree.h freelist.h darc.h cache.h lrulist.h lru.h hbtree.h fhbtree.h bucket.h hbucket.h cachesize.h cachemem.h writebuf.h pbaref.h iotracereader.h filerecipe.h
	$(CC) $(CFLAGS) -c csize.cpp

darc.o:		darc.cpp darc.h cache.h lrulist.h util.h writebuf.h dedupconfig.h dedupn.h cachesize.h iotracereader.h storagenode.h metadata.h ibtree.h freelist.h lru.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h cachemem.h
	$(CC) $(CFLAGS) -c darc.cpp

dedup-stat.o:	dedup-stat.cpp dedupconfig.h dedupn.h hbtree.h freelist.h lru.h cache.h darc.h lrulist.h ibtree.h dupset.h iotracereader.h cachesize.h
	$(CC) $(CFLAGS) -c dedup-stat.cpp

dedup-stat2.o:	dedup-stat2.cpp dedupconfig.h dedupn.h iotracereader.h hbtree.h freelist.h lru.h cache.h darc.h lrulist.h ibtree.h cachesize.h cachemem.h writebuf.h
	$(CC) $(CFLAGS) -c dedup-stat2.cpp

dedup-stat3.o:	dedup-stat3.cpp dedupconfig.h dedupn.h iotracereader.h hbtree.h freelist.h lru.h cache.h darc.h lrulist.h ibtree.h cachesize.h cachemem.h writebuf.h
	$(CC) $(CFLAGS) -c dedup-stat3.cpp

dupset.o:	dupset.cpp dupset.h 
	$(CC) $(CFLAGS) -c dupset.cpp

examibtree.o:	examibtree.cpp dedupconfig.h dedupn.h ibtree.h freelist.h cache.h darc.h lrulist.h lru.h cachesize.h
	$(CC) $(CFLAGS) -c examibtree.cpp 

examfilerecipe.o:	examfilerecipe.cpp dedupn.h dedupconfig.h filerecipe.h freelist.h darc.h cache.h lrulist.h lru.h cachesize.h storagenode.h metadata.h ibtree.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h writebuf.h iotracereader.h hbucket.h pbaref.h bitmap.h cachemem.h
	$(CC) $(CFLAGS) -c examfilerecipe.cpp 

examhbtree.o:	examhbtree.cpp dedupconfig.h dedupn.h hbtree.h freelist.h cache.h lru.h darc.h lrulist.h cachesize.h
	$(CC) $(CFLAGS) -c examhbtree.cpp 

fblist.o:	fblist.cpp fblist.h cache.h
	$(CC) $(CFLAGS) -c fblist.cpp

filerecipe.o:	filerecipe.cpp dedupn.h dedupconfig.h lru.h cache.h filerecipe.h freelist.h darc.h lrulist.h cachemem.h writebuf.h cachesize.h storagenode.h metadata.h ibtree.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h iotracereader.h hbucket.h pbaref.h bitmap.h util.h
	$(CC) $(CFLAGS) -c filerecipe.cpp

fhbtree.o:	fhbtree.cpp dedupn.h dedupconfig.h freelist.h fhbtree.h darc.h cache.h lrulist.h lru.h cachemem.h writebuf.h cachesize.h util.h iotracereader.h storagenode.h metadata.h ibtree.h hbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h
	$(CC) $(CFLAGS) -c fhbtree.cpp

freelist.o:	freelist.cpp freelist.h
	$(CC) $(CFLAGS) -c freelist.cpp

hbtree.o:	hbtree.cpp dedupn.h dedupconfig.h freelist.h hbtree.h lru.h cache.h darc.h lrulist.h cachemem.h writebuf.h cachesize.h util.h iotracereader.h storagenode.h metadata.h ibtree.h fhbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h
	$(CC) $(CFLAGS) -c hbtree.cpp

hbucket.o:	hbucket.cpp hbucket.h freelist.h buckcomn.h lru.h cache.h darc.h lrulist.h ibtree.h util.h writebuf.h dedupconfig.h dedupn.h cachesize.h iotracereader.h storagenode.h metadata.h hbtree.h fhbtree.h fblist.h bucket.h pbaref.h bitmap.h cachemem.h
	$(CC) $(CFLAGS) -c hbucket.cpp
ibtree.o:	ibtree.cpp dedupn.h dedupconfig.h freelist.h ibtree.h darc.h cache.h lrulist.h lru.h cachemem.h writebuf.h cachesize.h util.h iotracereader.h storagenode.h metadata.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h
	$(CC) $(CFLAGS) -c ibtree.cpp

storagenode.o:	storagenode.cpp storagenode.h metadata.h ibtree.h freelist.h darc.h cache.h lrulist.h lru.h hbtree.h fhbtree.h fblist.h bucket.h dedupn.h dedupconfig.h buckcomn.h writebuf.h cachesize.h iotracereader.h hbucket.h pbaref.h bitmap.h cachemem.h
	$(CC) $(CFLAGS) -c storagenode.cpp

iotracereader4k.o:	iotracereader4k.cpp dedupn.h dedupconfig.h iotracereader.h
	$(CC) $(CFLAGS) -c iotracereader4k.cpp

iotracereader.o:	iotracereader.cpp dedupn.h dedupconfig.h iotracereader.h
	$(CC) $(CFLAGS) -c iotracereader.cpp

iotracereaderhome.o:	iotracereaderhome.cpp dedupn.h dedupconfig.h iotracereader.h
	$(CC) $(CFLAGS) -c iotracereaderhome.cpp

lru.o:		lru.cpp cachemem.h cache.h writebuf.h dedupconfig.h dedupn.h cachesize.h lru.h util.h iotracereader.h storagenode.h metadata.h ibtree.h freelist.h darc.h lrulist.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h
	$(CC) $(CFLAGS) -c lru.cpp

lrulist.o:	lrulist.cpp cachemem.h cache.h writebuf.h dedupconfig.h dedupn.h cachesize.h lrulist.h util.h iotracereader.h storagenode.h metadata.h ibtree.h freelist.h darc.h lru.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h
	$(CC) $(CFLAGS) -c lrulist.cpp

main-15-app.o:	main-15-app.cpp dedupn.h dedupconfig.h metadata.h cache.h bucket.h hbtree.h fhbtree.h fblist.h buckcomn.h hbucket.h ibtree.h pbaref.h darc.h lru.h lrulist.h iotracereader.h iotraces.h util.h freelist.h writebuf.h cachesize.h bitmap.h cachemem.h
	$(CC) $(CFLAGS) -c main-15-app.cpp

main-15-bucket.o:	main-15-bucket.cpp dedupn.h dedupconfig.h metadata.h cache.h cachemem.h hbtree.h fhbtree.h fblist.h buckcomn.h bucket.h hbucket.h ibtree.h bitmap.h darc.h lru.h lrulist.h iotracereader.h iotraces.h util.h freelist.h writebuf.h cachesize.h pbaref.h 
	$(CC) $(CFLAGS) -c main-15-bucket.cpp

main-15-full.o:	main-15-full.cpp dedupn.h  dedupconfig.h metadata.h cache.h cachemem.h ibtree.h hbtree.h fhbtree.h fblist.h bucket.h hbucket.h pbaref.h darc.h lru.h lrulist.h iotracereader.h iotraces.h util.h bitmap.h 
	$(CC) $(CFLAGS) -c main-15-full.cpp

main-15-native.o:	main-15-native.cpp dedupn.h  dedupconfig.h iotracereader.h iotraces.h util.h writebuf.h cachesize.h cache.h cachemem.h lru.h darc.h lrulist.h
	$(CC) $(CFLAGS) -c main-15-native.cpp

main-client.o:	main-client.cpp dedupconfig.h dedupn.h cachemem.h cache.h writebuf.h cachesize.h bitmap.h metadata.h ibtree.h freelist.h darc.h lrulist.h lru.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h iotracereader.h hbucket.h pbaref.h util.h storagenode.h iotraces.h filerecipe.h
	$(CC) $(CFLAGS) -c main-client.cpp

main-extbin.o:	main-extbin.cpp dedupconfig.h dedupn.h cachemem.h cache.h writebuf.h cachesize.h bitmap.h metadata.h ibtree.h freelist.h darc.h lrulist.h lru.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h iotracereader.h hbucket.h pbaref.h util.h storagenode.h iotraces.h filerecipe.h
	$(CC) $(CFLAGS) -c main-extbin.cpp

md5.o:	md5.cpp
	$(CC) $(CFLAGS) -c md5.cpp

metadata.o:	metadata.cpp dedupconfig.h dedupn.h metadata.h ibtree.h freelist.h darc.h cache.h lrulist.h lru.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h writebuf.h cachesize.h iotracereader.h hbucket.h pbaref.h
	$(CC) $(CFLAGS) -c metadata.cpp

mkbitmap.o:	mkbitmap.cpp bitmap.h dedupconfig.h dedupn.h
	$(CC) $(CFLAGS) -c mkbitmap.cpp

mkpbaref.o:	mkpbaref.cpp pbaref.h 
	$(CC) $(CFLAGS) -c mkpbaref.cpp

pbaref.o:	pbaref.cpp dedupn.h dedupconfig.h pbaref.h cachemem.h cache.h writebuf.h cachesize.h bitmap.h hbtree.h freelist.h lru.h darc.h lrulist.h metadata.h ibtree.h fhbtree.h fblist.h bucket.h buckcomn.h iotracereader.h hbucket.h storagenode.h
	$(CC) $(CFLAGS) -c pbaref.cpp

readgen.o:	readgen.cpp dedupn.h dedupconfig.h iotracereader.h
	$(CC) $(CFLAGS) -c readgen.cpp

readgen2.o:	readgen2.cpp dedupn.h dedupconfig.h iotracereader.h
	$(CC) $(CFLAGS) -c readgen2.cpp

util.o:	util.cpp dedupn.h dedupconfig.h cache.h util.h writebuf.h cachesize.h iotracereader.h storagenode.h metadata.h ibtree.h freelist.h darc.h lrulist.h lru.h hbtree.h fhbtree.h fblist.h bucket.h buckcomn.h hbucket.h pbaref.h bitmap.h cachemem.h
	$(CC) $(CFLAGS) -c util.cpp

writebuf.o:	writebuf.cpp dedupn.h dedupconfig.h writebuf.h cachesize.h
	$(CC) $(CFLAGS) -c writebuf.cpp


#Cleaning the object files and all binaries

clean:
	rm *.o

cleanall:
	rm *.o dedup-15-full dedup-15-bucket dedup-15-app ddeup-app native-15-response dedup-stat dedup-stat2 dedup-stat3 content-stat content-stat2 convert-trace readgen readgen2 csize hashtest mkindex mkbucket mkbitmap mkpbaref mkfblist examibtree examhbtree exambucket examhbucket examtrace 


