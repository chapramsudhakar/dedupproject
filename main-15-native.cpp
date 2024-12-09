/**********************************************************************
 *  This program is to find native response time		      *
 *  and other corresponding performance parameters.                   *
 **********************************************************************/
#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "dedupconfig.h"
#include "dedupn.h"
#include "cache.h"
#include "cachemem.h"
#include "iotracereader.h"
#include "util.h"
#include "darc.h"
#include "lru.h"
//#include "metadata.h"

#include "iotraces.h"

//extern unsigned long int DISK_MILLE;

IOTraceReader *trace[10];

void cacheMemStats(void);


//#ifdef  DARCP
//#ifdef  HOME
////DARC dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE_NATIVE, 512);
//DARC dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE, 512);
//#else
////DARC dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE_NATIVE, 4096);
//DARC dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
//#endif
//#else
////LRU dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE_NATIVE);
//LRU dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE);
//#endif

string dummyfile="/dev/null";
fstream dummy;

unsigned int reccount100000 = 1;
unsigned long long int curTime;

/************************************************/
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_READ_COUNT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_WRITE_COUNT = 0;
//unsigned long long int DATA_BLOCK_EFFECTIVE_WRITES = 0;
//unsigned long long int DATA_BLOCK_FLUSH_WRITES = 0;

//unsigned long long int UNIQUE_BLOCK_COUNT = 0;

//unsigned long long int DISK_READ_COUNT[2] = {0, 0};
//unsigned long long int DISK_READ_BCOUNT[2] = {0, 0};
//unsigned long long int DISK_WRITE_COUNT[2] = {0, 0};
//unsigned long long int DISK_WRITE_BCOUNT[2] = {0, 0};

//double DISK_READ_TOT_TIME[2] = {0, 0};		// Time in mille seconds
//double DISK_WRITE_TOT_TIME[2] = {0, 0};
//double DISK_READ_MIN_TIME[2] = {10000, 10000};	// Time in mille seconds
//double DISK_READ_MAX_TIME[2] = {0, 0};
//double DISK_WRITE_MIN_TIME[2] = {10000, 10000};	// Time in mille seconds
//double DISK_WRITE_MAX_TIME[2] = {0, 0};
//unsigned long int DISK_READ_REQ_COUNT[1028][2000];
//unsigned long int DISK_WRITE_REQ_COUNT[1028][2000];
//unsigned long int DISK_IWRITE_REQ_COUNT[1028][2000];
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

//unsigned long long int FP_CACHE_HIT = 0;
//unsigned long long int FP_CACHE_MISS = 0;
//unsigned long long int LBA2PBA_CACHE_HIT = 0;
//unsigned long long int LBA2PBA_CACHE_MISS = 0;
//unsigned long long int PBAREF_CACHE_HIT = 0;
//unsigned long long int PBAREF_CACHE_MISS = 0;


//unsigned long long int BTSEARCH = 0;
//unsigned long long int BTINSERT = 0;
//unsigned long long int BTDELETE = 0;
//unsigned long long int BTUPDATE = 0;

//unsigned long long int HBTSEARCH = 0;
//unsigned long long int HBTINSERT = 0;
//unsigned long long int HBTDELETE = 0;
//unsigned long long int HBTUPDATE = 0;


//unsigned long long int PBREFSEARCH = 0;
//unsigned long long int PBREFINSERT = 0;
//unsigned long long int PBREFDELETE = 0;
//unsigned long long int PBREFUPDATE = 0;

//unsigned long long int PBREF_READCOUNT_READ = 0;
//unsigned long long int PBREF_WRITECOUNT_READ = 0;
//unsigned long long int PBREF_READCOUNT_WRITE = 0;
//unsigned long long int PBREF_WRITECOUNT_WRITE = 0;
int OPERATION;
/************************************************/
double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;
//double t1_disk0 = 0, t1_disk1 = 0, t2_disk0 = 0, t2_disk1 = 0;
//double t3_disk0 = 0, t3_disk1 = 0;
//double t1_clk = 0, t2_clk = 0, t3_clk = 0, t0_hash = 0;

/*
 * sn^2 = (n-2)/(n-1)*sn-1^2 + 1/n*(xn - mean(xn-1)^2
 * mean(xn) = ((n-1)*mean(xn-1) + xn) / n;
 */



/************************************************/
//unsigned long int prevblock[2];	// Two disks
//MDMaps *md = NULL;

void update_statistics(double var[], double mean[], 
		double minimum[], double maximum[],
		unsigned long int  N[], int seglen, unsigned int microsec) {
	unsigned long int n;

	n = ++N[seglen-1];
	if (n > 1) {
		var[seglen-1] = ((n-2)*var[seglen-1]/(n-1)) +
			((microsec - mean[seglen-1])*
			 (microsec - mean[seglen-1])/n);
		mean[seglen-1] = ((n-1)*mean[seglen-1] + microsec) / n;

		if (minimum[seglen-1] > microsec) minimum[seglen-1] = microsec;
		if (maximum[seglen-1] < microsec) maximum[seglen-1] = microsec;
	}
	else {
		var[seglen-1] = 0.0;
		mean[seglen-1] = microsec;
		minimum[seglen-1] = microsec;
		maximum[seglen-1] = microsec;
	}
}

void processReadTrace(int tno) {
	IOTraceReader *trace;
	struct segment s;
	struct segment4k s1;
	struct segmenthome s2;
	int next_pid = -1;
	int i, j, k, m; 
	int scount, op; 
	unsigned int pba1, pba2;
	CacheEntry *c, *cent; 
	unsigned long long int rtime, wtime;
	//unsigned int lastFlushTime = 0;
	//unsigned int lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	//unsigned int twritecount;
	unsigned short rwflag;
	int retval; //, trace15 = 0;

	unsigned long int ind;
	unsigned long int overhead, dtime;
	int seg_size;

	reccount100000 = 1;	
	if (tno == 2)
		trace = new IOTraceReaderHome();
	else    trace = new IOTraceReader4K();

	if ((trace->openTrace(frnames[tno], fbsize[tno]) != 0)) {
		cout << frnames[tno] <<":";
		perror("IO Trace opening failure:");
		exit(0);
	}

	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	//cout << " sizeof s1 : " << sizeof(s1) << endl;
	while (1) {
		if (tno != 2)
			retval = trace->readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		else
			retval = trace->readNextSegment(&next_pid, &op, &ts1, &ts2, &s2);

		if (retval == 0) break;

		READ_REQ_COUNT++;
		OPERATION = OPERATION_READ;

#ifndef HOME
		DATA_BLOCK_READ_COUNT += (s1.seg_size * 8);
		s1.seg_start /= 8;
		if (s1.seg_size > 1)
			incr = (ts2 - ts1) / (s1.seg_size - 1);
		else	incr = 2;
		seg_size = s1.seg_size;
#else
		DATA_BLOCK_READ_COUNT += s2.seg_size;
		if (s2.seg_size > 1)
			incr = (ts2 - ts1) / (s2.seg_size - 1);
		else	incr = 2;
		seg_size = s2.seg_size;
#endif
		curTime = ts1;
		if (incr <= 0) incr = 2;

		clock_start();
		disk_start();
		
		m = 0;
		// Split and process home requests which are very larger
		while (m < seg_size) {
#ifndef	HOME
			s.seg_start = s1.seg_start + m;
			for (i=0; (i<1) && ((m + i) < s1.seg_size); i++)
				memcpy(s.seg_hash[i], s1.seg_hash[m+i], 16);
			s.seg_size = i;
#else
			s.seg_start = s2.seg_start + m;
			for (i=0; (i<1) && ((m + i) < s2.seg_size); i++)
				memcpy(s.seg_hash[i], s2.seg_hash[m+i], 16);
			s.seg_size = i;
#endif


			// Search for the data blocks segment by segment 
			// in the cache with the representative pba number
			i = 0;
			rtime = wtime = 0;
			rtime = ts1;
			for (j=0; j<s.seg_size; j++) {
				pba1 = s.seg_start + j;
				c = dc.searchCache(pba1);
				if (c != NULL) {
#ifndef	HOME
					DATA_BLOCK_READ_CACHE_HIT += 8;
#else
					DATA_BLOCK_READ_CACHE_HIT++;
#endif
					c->ce_rtime = rtime;
					c->ce_refcount++;
					dc.c_rrefs++;
#ifdef  DARCP
					dc.repositionEntry(c);
#endif
					rtime += incr; 
					curTime += incr;
				}
				else {
					// Find the sequence of blocks not in 
					// data cache, and issue single request
					scount = 1;
					pba2 = pba1+1;
					j++;

					while ((j < s.seg_size) && ((c = dc.searchCache(pba2)) == NULL)) {
						j++; scount++; pba2++;
					}
					j--; //Go back to the last block
						

#ifndef	HOME
					DiskRead(curTime, DATADISK, 8*(pba1), scount*8);
#else
					DiskRead(curTime, DATADISK, pba1, scount);
#endif

					rwflag = CACHE_ENTRY_CLEAN;
					curTime += (incr * scount);
					// Add missing data block entries to the cache
					for (k=0; k<scount; k++) {
						pba2 = pba1 + k;

#ifndef	HOME
						DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 4096, rtime, wtime, DATADISK, rwflag, NULL, &cent);
#else
						DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 512, rtime, wtime, DATADISK, rwflag, NULL, &cent);
#endif
					}
					rtime += (incr * scount); 
				}
			}
			ts1 += (incr * s.seg_size);
			m += s.seg_size;
		}

		overhead = clock_stop();
		dtime = disk_stop();

		ind = dtime*1000 + overhead;	// Microseconds
#ifdef	HOME
		update_statistics(r_var, r_mean, r_min, r_max, r_n, seg_size, ind);
#else
		update_statistics(r_var, r_mean, r_min, r_max, r_n, seg_size*8, ind);
#endif
		//ind /= 10;
/*
		if (ind >= 2000) {
#ifdef	__DEBUG__
			cout << "Request processing time exceeded " << ind << endl;
#endif
			ind = 1999;
		}
#ifndef	HOME
		DISK_READ_REQ_COUNT[s1.seg_size-1][ind]++;
#else
		DISK_READ_REQ_COUNT[s2.seg_size-1][ind]++;
#endif
*/
		if (trace->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace->getCurCount() << "  number of read records processed" << endl;
			reccount100000++;
		}

		//if (lastGCTime == 0) { 
			// Not yet initialized
			//lastFlushTime = ts2 / 1000000;// Milleseconds
			//lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			//cout << "flushing 30 "  
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			//cout << "flushing 75% \n";
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef  DARCP
		// New interval starting, once per every 10 minutes
		//if (((ts2 / 1000000) - lastDARCTime) > (60000 * 10)) {
		if (((darcInterval + 1) * DARCINTREFCOUNT) >
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
			dc.newInterval();
			darcInterval++;
			//lastDARCTime = ts2 / 1000000;
		}

		if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
			lastDecayTime = ts2 / 1000000;
			//dc.decayWeights();
			//darcDecayInterval++;
		}

#endif

		// Garbage collection once in an hour
		//if (((ts2 / 1000000) - lastGCTime) > (60000 * 60)) {
			//gc();
			//lastGCTime = ts2 / 1000000;
		//}
	}
	cout << "\n\nTotal read records processed \t\t\t: " << trace->getCurCount() << endl;
}

int main(int argc, char** argv) {
	int next_pid = -1;
	int i, j, k, m; 
	int scount, op; 
	unsigned int pba1, pba2;
	CacheEntry *c, *cent; 
	unsigned long long int rtime, wtime;
	//unsigned int lastFlushTime = 0;
	//unsigned int lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct segment s;
	struct segment4k s1;
	struct segmenthome s2;
	unsigned int twritecount;
	unsigned short rwflag;
	int retval; //, trace15 = 0;

	unsigned long int ind;
	unsigned long int overhead, dtime;
	int tno = 0;
	int seg_size;
	struct timespec tval;

#ifdef  DARCP
	unsigned long int DARCINTREFCOUNT;
	unsigned int darcInterval = 0;

	DARCINTREFCOUNT = 512 * (DATA_CACHE_SIZE_NATIVE / 512);
	unsigned long long int lastDecayTime = 0;
#endif

#ifdef  MAIL
	tno = 0;
#endif
#ifdef  WEB
	tno = 1;
#endif
#ifdef  HOME
	tno = 2;
#endif
#ifdef  LINUX
        tno = 3;
#endif
#ifdef  BOOKPPT
        tno = 4;
#endif
#ifdef  VIDIMG
        tno = 5;
#endif

	initCacheMem();

	dummy.open(dummyfile.c_str(), ios::in | ios::out | ios::binary);

/*	trace[0] = new IOTraceReader4K();
	trace[1] = new IOTraceReader4K();
	trace[2] = new IOTraceReaderHome();

	//if (trace.openTrace(fnames[tno], 14) != 0) {
	//if (trace.openTrace(fnames[tno], 1) != 0) {
	if ((trace[0]->openTrace(fnames[0], 21, 4096) != 0) ||
		(trace[1]->openTrace(fnames[1], 21, 4096) != 0) ||
		(trace[2]->openTrace(fnames[2], 21, 512) != 0)) {
		perror("IO Trace opening failure:");
		exit(0);
	}
*/
	//if (trace.openTrace(fnames[tno], 14) != 0) {
	//if (trace.openTrace(fnames[tno], 21) != 0) {
		//perror("IO Trace opening failure:");
		//exit(0);
	//}

//	if (trace.openTrace(fnames[tno], 1) != 0) {
//		perror("IO Trace opening failure:");
//		exit(0);
//	}

	for (i=0; i<6; i++) {
		if (i == 2)
			trace[i] = new IOTraceReaderHome();
		else    trace[i] = new IOTraceReader4K();

		if ((trace[i]->openTrace(fnames[i], fcount[i],
			fbsize[i]) != 0)) {
			cout << fnames[i][0] <<":";
			perror("IO Trace opening failure:");
			exit(0);
		}
	}

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t1_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);

	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	//cout << " sizeof s1 : " << sizeof(s1) << endl;
	while (1) {
		if (tno != 2)
			retval = trace[tno]->readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		else
			retval = trace[tno]->readNextSegment(&next_pid, &op, &ts1, &ts2, &s2);

		if (retval == 0) break;

#ifdef	__DEBUG__
		cout <<  "\n\nseg : " << s1.seg_start << ", " << s1.seg_size <<  " timestamps " << ts1/1000000 << " " << ts2/1000000 << endl;
#endif

		if (op == OP_READ) {
			READ_REQ_COUNT++;
			OPERATION = OPERATION_READ;
#ifndef	HOME
			DATA_BLOCK_READ_COUNT += (s1.seg_size * 8);
#else
			DATA_BLOCK_READ_COUNT += s2.seg_size;
#endif
		}
		else {
			WRITE_REQ_COUNT++;
			OPERATION = OPERATION_WRITE;
#ifndef	HOME
			DATA_BLOCK_WRITE_COUNT += (s1.seg_size * 8);
#else
			DATA_BLOCK_WRITE_COUNT += s2.seg_size;
#endif
		}
#ifndef HOME
		s1.seg_start /= 8;
		if (s1.seg_size > 1)
			incr = (ts2 - ts1) / (s1.seg_size - 1);
		else	incr = 2;
		seg_size = s1.seg_size;
#else
		if (s2.seg_size > 1)
			incr = (ts2 - ts1) / (s2.seg_size - 1);
		else	incr = 2;
		seg_size = s2.seg_size;
#endif
		curTime = ts1;
		if (incr <= 0) incr = 2;

		clock_start();
		disk_start();
		
		m = 0;
		// Split and process home requests which are very larger
		while (m < seg_size) {
#ifndef	HOME
			s.seg_start = s1.seg_start + m;
			for (i=0; (i<1) && ((m + i) < s1.seg_size); i++)
				memcpy(s.seg_hash[i], s1.seg_hash[m+i], 16);
			s.seg_size = i;
#else
			s.seg_start = s2.seg_start + m;
			for (i=0; (i<1) && ((m + i) < s2.seg_size); i++)
				memcpy(s.seg_hash[i], s2.seg_hash[m+i], 16);
			s.seg_size = i;
#endif

#ifdef	__DEBUG__
			cout <<  "\t\tseg : " << s.seg_start << " , " << s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

			// Search for the data blocks segment by segment 
			// in the cache with the representative pba number
			i = 0;
			rtime = wtime = 0;
			if (op == OP_READ) rtime = ts1;
			else wtime = ts1;
			for (j=0; j<s.seg_size; j++) {
				pba1 = s.seg_start + j;
				c = dc.searchCache(pba1);
				if (c != NULL) {
					if (op == OP_READ) {
#ifndef	HOME
						DATA_BLOCK_READ_CACHE_HIT += 8;
#else
						DATA_BLOCK_READ_CACHE_HIT++;
#endif
						c->ce_rtime = rtime;
						c->ce_refcount++;
						dc.c_rrefs++;
#ifdef  DARCP
						dc.repositionEntry(c);
#endif
						rtime += incr; 
					}
					else {
						cout << "WRite cache hit\n";
#ifndef	HOME
						DATA_BLOCK_WRITE_CACHE_HIT += 8;
#else
						DATA_BLOCK_WRITE_CACHE_HIT++;
#endif
						c->ce_wtime = wtime;
						c->ce_refcount++;
						c->ce_wrefcount++;
						dc.c_wrefs++;
						if ((c->ce_flags & CACHE_ENTRY_DIRTY) == 0) {
							c->ce_flags |= CACHE_ENTRY_DIRTY;
							dc.c_dcount++;
						}
#ifdef  DARCP
						dc.repositionEntry(c);
#endif
						wtime += incr;
					}
					curTime += incr;
				}
				else {
					// Find the sequence of blocks not in 
					// data cache, and issue single request
					scount = 1;
					pba2 = pba1+1;
					j++;

					while ((j < s.seg_size) && ((c = dc.searchCache(pba2)) == NULL)) {
						j++; scount++; pba2++;
					}
					j--; //Go back to the last block
						
					if (op == OP_READ) {

#ifndef	HOME
						DiskRead(curTime, DATADISK, 8*(pba1), scount*8);
#else
						DiskRead(curTime, DATADISK, pba1, scount);
#endif

						rwflag = CACHE_ENTRY_CLEAN;
					}
					else {
						rwflag = CACHE_ENTRY_DIRTY;
					}
					curTime += (incr * scount);
					// Add missing data block entries to the cache
					for (k=0; k<scount; k++) {
						pba2 = pba1 + k;

#ifndef	HOME
						DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 4096, rtime, wtime, DATADISK, rwflag, NULL, &cent);
#else
						DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 512, rtime, wtime, DATADISK, rwflag, NULL, &cent);
#endif
					}
					if (op == OP_READ) rtime += (incr * scount); 
					else wtime += (incr * scount);
				}
			}
			ts1 += (incr * s.seg_size);
			m += s.seg_size;
		}

		overhead = clock_stop();
		dtime = disk_stop();

		ind = dtime*1000 + overhead; // Microseconds
#ifdef	HOME
		update_statistics(w_var, w_mean, w_min, w_max, w_n, seg_size, ind);
#else
		update_statistics(w_var, w_mean, w_min, w_max, w_n, seg_size*8, ind);
#endif

/*
		//ind /= 10;
		if (ind >= 2000) {
#ifdef	__DEBUG__
			cout << "Request processing time exceeded " << ind << endl;
#endif
			ind = 1999;
		}
#ifndef	HOME
		if (op == OP_READ) 
			DISK_READ_REQ_COUNT[s1.seg_size-1][ind]++;
		else
			DISK_WRITE_REQ_COUNT[s1.seg_size-1][ind]++;
#else
		if (op == OP_READ) 
			DISK_READ_REQ_COUNT[s2.seg_size-1][ind]++;
		else
			DISK_WRITE_REQ_COUNT[s2.seg_size-1][ind]++;
#endif
*/
		if (trace[tno]->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace[tno]->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

		//if (lastGCTime == 0) { 
			// Not yet initialized
			//lastFlushTime = ts2 / 1000000;// Milleseconds
			//lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			//cout << "flushing 30 "  
			// Dummy writing ...
			clock_start();
			disk_start();

			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);

			overhead = clock_stop();
			dtime = disk_stop();

			ind = dtime*1000 + overhead;	// Microseconds
			w_flush += ind;
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			//cout << "flushing 75% \n";
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef  DARCP
		// New interval starting, once per every 10 minutes
		//if (((ts2 / 1000000) - lastDARCTime) > (60000 * 10)) {
		if (((darcInterval + 1) * DARCINTREFCOUNT) >
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
			dc.newInterval();
			darcInterval++;
			//lastDARCTime = ts2 / 1000000;
		}

		if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
			lastDecayTime = ts2 / 1000000;
			//dc.decayWeights();
			//darcDecayInterval++;
		}

#endif

		// Garbage collection once in an hour
		//if (((ts2 / 1000000) - lastGCTime) > (60000 * 60)) {
			//gc();
			//lastGCTime = ts2 / 1000000;
		//}
	}

	clock_start();
	disk_start();

	twritecount = dc.flushCache(dummy, 0, DATADISK);
	DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
	DATA_BLOCK_FLUSH_WRITES += twritecount;


	overhead = clock_stop();
	dtime = disk_stop();

	ind = dtime*1000 + overhead;	// Microseconds
	w_flush += ind;

	cout << "\n\nTotal write records processed \t\t\t: " << trace[tno]->getCurCount() << endl;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t2_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t2_disk0 = DISK_WRITE_TOT_TIME[0];
       	t2_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	//============================

	cout << "\n=========== READ Trace (" 
		<< frnames[tno] << ") processing =========\n\n";
	processReadTrace(tno);
	
	//============================

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t3_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t3_disk0 = DISK_READ_TOT_TIME[0];
       	t3_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	cout.setf(ios::fixed);
	cout.precision(4);

	cout << "\nt1_clk " << t1_clk << "\nt2_clk " << t2_clk << "\nt3_clk " << t3_clk << "\nt2_disk0 " << t2_disk0 << "\nt3_disk0 " << t3_disk0 << "\nt2_disk1 " << t2_disk1 << "\nt3_disk1 " << t3_disk1 << "\nt0_hash " << t0_hash << endl;

	cout << "Write throughput : " << (((DATA_BLOCK_WRITE_COUNT * 512.0 * 
		10000000.0) / (t2_clk - t1_clk + ((t2_disk0 + t2_disk1)*1000))) 
		/ (1024 * 1024)) << "MB / sec\n";
	cout << "Read throughput : " << (((DATA_BLOCK_READ_COUNT * 512.0 * 
		10000000.0) / (t3_clk - t2_clk + 
		((t3_disk0 + t3_disk1 - t2_disk1)*1000))) 
		/ (1024 * 1024)) << "MB / sec\n";

	cout << "Working set size : " << WSS << "KB" << endl;

	cout << "Cache sizes used:\n" << "Data Cache size \t\t: "
                << (DATA_CACHE_SIZE / (1024))
		<< " KB\n";
                //<< (DATA_CACHE_SIZE_NATIVE / (1024))

	cacheMemStats();

	for (i=0; i<2; i++) {
		cout << "\n\nDISK : " << i << endl;
		cout << "\nDISK_READ_COUNT \t: " << DISK_READ_COUNT[i] << endl
		<< "DISK_WRITE_COUNT \t: " << DISK_WRITE_COUNT[i] << endl
		<< "DISK_READ_BCOUNT \t: " << DISK_READ_BCOUNT[i] << endl
		<< "DISK_WRITE_BCOUNT \t: " << DISK_WRITE_BCOUNT[i] << endl
		<< "DISK_READ_TOT_TIME \t: " << DISK_READ_TOT_TIME[i] << endl
		<< "DISK_WRITE_TOT_TIME \t: " << DISK_WRITE_TOT_TIME[i] << endl;
	}
		
	cout << "\n\nDATA READ COUNT \t\t: " << DATA_BLOCK_READ_COUNT << endl
		<< "DATA READ CACHE HITS \t\t: " << DATA_BLOCK_READ_CACHE_HIT << endl
		<< "DATA WRITE COUNT \t\t: " << DATA_BLOCK_WRITE_COUNT << endl
		<< "DATA WRITE CACHE HITS \t\t: " << DATA_BLOCK_WRITE_CACHE_HIT << endl
		<< "DATA BLOCK EFFECTIVE READS \t: " << (DATA_BLOCK_READ_COUNT - DATA_BLOCK_READ_CACHE_HIT) << endl
		<< "DATA BLOCK EFFECTIVE WRITES \t: " << DATA_BLOCK_EFFECTIVE_WRITES << endl
		<< "DATA BLOCK FLUSH WRITES \t: " << DATA_BLOCK_FLUSH_WRITES << endl;
		

	//cout << "\n\nTotal I/O records processed \t\t\t: " << trace[tno]->getCurCount() << endl;

	display_io_response();

	cout << "\n\nData cache statistics \n";
	dc.displayStatistics();

	trace[tno]->closeTrace();	

	return 0;
}



