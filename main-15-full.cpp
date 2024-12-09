/**********************************************************************
 *  This program is to perform complete deduplication and measure the *
 *  corresponding performance parameters.                             *
 *								      *
 **********************************************************************/
#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include "dedupconfig.h"
#include "dedupn.h"
//#include "ibtree.h"
//#include "hbtree.h"
//#include "pbaref.h"
#include "bitmap.h"
//#include "darc.h"
//#include "lru.h"
#include "metadata.h"
#include "iotracereader.h"
#include "util.h"
#include "cachemem.h"
#include "iotraces.h"

void cacheMemStats(void);


//#ifdef	DARCP
//#ifdef	HOME
//DARC dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE, 512);
//#else
//DARC dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
//#endif
//#else
//LRU dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE);
//#endif

string dummyfile="/dev/null";
fstream dummy;

IOTraceReader	*trace[10];

unsigned int reccount100000 = 1;
unsigned long long int curTime;

/************************************************/
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS = 0;
//unsigned long long int DATA_BLOCK_READ_COUNT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_MISS = 0;
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
////double DISK_READ_MIN_TIME[2] = {10000, 10000};	// Time in mille seconds
//double DISK_READ_MAX_TIME[2] = {0, 0};
//double DISK_WRITE_MIN_TIME[2] = {10000, 10000};	// Time in mille seconds
//double DISK_WRITE_MAX_TIME[2] = {0, 0};
//unsigned long int DISK_READ_REQ_COUNT[1028][1000];
//unsigned long int DISK_WRITE_REQ_COUNT[1028][1000];
//unsigned long int DISK_IWRITE_REQ_COUNT[1028][1000];
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

double SEGCOUNT3 = 0.0;
double SEGLENGTH3 = 0.0;
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


//unsigned long int prevblock[2];	// Two disks

//MDMaps *md;

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

void countSegmentLength(unsigned int pba[], unsigned int seg_size) {
	unsigned int i;
	int diff;

	SEGLENGTH3 += seg_size;

	// Already not existing
	i = 1;
	while (i<seg_size) {

		diff = pba[i] - pba[i-1];

		if (abs(diff) > 2) {
			SEGCOUNT3++;
		}
		i++;

	}

	SEGCOUNT3++;    // Last segment
}


// Apply deduplication information and find the pba addresses of
// the read requests lba's
void dedupReadRequest(struct segment *s, MDMaps *m, unsigned int pba[]) {
	int i; 
	unsigned int lba; 

	// If the operation is read try to find the physical 
	// block addresses. If not present it can be assumed 
	// that the blocks were written before and now being 
	// read. So for simulation purpose find any duplicate 
	// blocks exist with the same hash value. If not 
	// existing, then allocate the physical blocks.

	//dupcnt = s->seg_size;
	for (i=0; i<s->seg_size; i++) {
		lba = s->seg_start + i;
		m->bt->searchValWrap(lba, &pba[i]);

		if (pba[i] == 0) {
			OPERATION = OPERATION_WRITE;
			// Not found in the lba index
			// deduplicate this lba read
			m->hbt->searchValWrap(s->seg_hash[i], &pba[i]);

			if (pba[i] == 0) {
				// Not found in hash index also
				// Add an entry to hash index
				pba[i] = allocBlocks(1); 
				UNIQUE_BLOCK_COUNT++;
				assert(pba[i] > 0);
				m->hbt->insertValWrap(s->seg_hash[i], pba[i]);
			}

			// Add an entry to lba index
			m->bt->insertValWrap(lba, pba[i]);

			// Increment the pba reference count
			m->pbaref->incrementPbaRef(pba[i], s->seg_hash[i]);
			OPERATION = OPERATION_READ;
		}
	}
}

void dedupWriteRequest(struct segment *s, MDMaps *m, unsigned int pba[]) {
	int i; 
	unsigned int pba2, lba; 

	//dupcnt = s->seg_size;

	// If the operation is write try to find the duplicate 
	// physical block addresses within finger print cache. 
	// If not present search the hash index. If there also
	// not found then add new entry to hash index.
	// Search the lba index also. If exists and finger 
	// print are different, then update the finger print.
	// If not present then add a new entry.

#ifdef	HOME
	clock_add(s->seg_size*4);
	//t0_hash += (s->seg_size*4);
#else
	clock_add(s->seg_size*30);
	//t0_hash += (s->seg_size*30);
#endif
	for (i=0; i<s->seg_size; i++) {
		lba = s->seg_start + i;

		// Search for the duplicate hash in hash index
		m->hbt->searchValWrap(s->seg_hash[i], &pba[i]);

		if (pba[i] == 0) {
			// New one, Allocate a block and add hash entry
			pba[i] = allocBlocks(1); 
			UNIQUE_BLOCK_COUNT++;
			assert(pba[i] > 0);
			m->hbt->insertValWrap(s->seg_hash[i], pba[i]);
			//dupcnt--;
		}

		// Get the old pba for this lba if exists
		m->bt->searchValWrap(lba, &pba2);

		if (pba2 == 0) {
			m->bt->insertValWrap(lba, pba[i]);
			m->pbaref->incrementPbaRef(pba[i], s->seg_hash[i]);
		}
		else if (pba[i] != pba2) {
			m->pbaref->decrementPbaRef(pba2, m);
			//m->bt->deleteValWrap(lba);
			m->bt->updateValWrap(lba, pba[i]);
			m->pbaref->incrementPbaRef(pba[i], s->seg_hash[i]);
		}
	}
	countSegmentLength(pba, s->seg_size);
}

void processReadTrace(int tno, MDMaps *md) {
	IOTraceReader *trace;

	int next_pid = -1;
	int i, j, k, m; 
	int count = 0, scount, op; 
	unsigned int pba1, pba2, pba[MAX_SEG_SIZE], pbastart;
	CacheEntry *c; 
	unsigned long long int rtime, wtime;
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct segment s;
	struct segmenthome s2;
	struct segment4k s1;
	//unsigned int twritecount;
	unsigned short rwflag;
	int retval; //, trace15 = 0;
	unsigned long int ind;
	unsigned long int overhead, dtime;
        //int tno;
	int seg_size;
	//struct timespec tval;

#ifdef  DARCP
	unsigned long int DARCINTREFCOUNT;
	unsigned int darcInterval = 0;

	DARCINTREFCOUNT = 512*(DATA_CACHE_SIZE / 512);
	unsigned long long int lastDecayTime = 0;
#endif

	reccount100000 = 1;

	if (tno == 2)
		trace = new IOTraceReaderHome();
	else    trace = new IOTraceReader4K();

	if ((trace->openTrace(frnames[tno], fbsize[tno]) != 0)) {
		perror("IO Trace opening failure:");
		exit(0);
	}

	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	while (1) {
		//retval = trace.readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		if (tno != 2) // Not home trace
			retval = trace->readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		else
			retval = trace->readNextSegment(&next_pid, &op, &ts1, &ts2, &s2);

		if (retval == 0) break;

#ifdef	__DEBUG__
		cout <<  "\n\nseg : " << s1.seg_start << ", " << s1.seg_size <<  " timestamps " << ts1/1000000 << " " << ts2/1000000 << endl;
#endif

		READ_REQ_COUNT++;
		OPERATION = OPERATION_READ;
#ifndef	HOME
		DATA_BLOCK_READ_COUNT += (s1.seg_size * 8);
#else
		DATA_BLOCK_READ_COUNT += s2.seg_size;
#endif
		
#ifndef HOME
		s1.seg_start /= 8;
		if (s1.seg_size > 1)
			incr = (ts2 - ts1) / (s1.seg_size - 1);
		else 	incr = 2;
		seg_size = s1.seg_size;
#else
		if (s2.seg_size > 1)
			incr = (ts2 - ts1) / (s2.seg_size - 1);
		else 	incr = 2;
		seg_size = s2.seg_size;
#endif
		curTime = ts1;
		if (incr <= 0) incr = 2;

		clock_start();
		disk_start();

		m = 0;

		// Split and process home requests which are very larger
		while (m < seg_size) {
#ifndef HOME
			s.seg_start = s1.seg_start + m;
			for (i=0; (i<MAX_SEG_SIZE) && ((m + i) < s1.seg_size); i++)
				memcpy(s.seg_hash[i], s1.seg_hash[m+i], 16);
			s.seg_size = i;
#else
			s.seg_start = s2.seg_start + m;
			for (i=0; (i<MAX_SEG_SIZE) && ((m + i) < s2.seg_size); i++)
				memcpy(s.seg_hash[i], s2.seg_hash[m+i], 16);
			s.seg_size = i;
#endif

#ifdef	__DEBUG__
			cout <<  "\t\tseg : " << s.seg_start << " , " << s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

			dedupReadRequest(&s, md, pba);
		
			// Search for the data blocks segment by segment 
			// in the cache with the representative pba number
			i = 0;
			rtime = wtime = 0;
			rtime = ts1;
			while (i < s.seg_size) {
				pbastart = pba[i];
				count = 1;
				i++;
				while ((i < s.seg_size) && (pba[i] == pba[i-1] + 1)) {
					count++;
					i++;
				}

				// Search for the whole segment?
				for (j=0; j<count; j++) {
					pba1 = pbastart + j;
					c = dc.searchCache(pba1);
					if (c != NULL) {
#ifndef	HOME
						DATA_BLOCK_READ_CACHE_HIT += 8;
#else
						DATA_BLOCK_READ_CACHE_HIT++;
#endif
						c->ce_refcount++;
						dc.c_rrefs++;
						c->ce_rtime = rtime;
						rtime += incr; 
#ifdef	DARCP
						dc.repositionEntry(c);
#endif
						curTime += incr;
					}
					else {
						// Find the sequence of blocks not in 
						// data cache, and issue single request
						scount = 1;
						pba2 = pba1+1;
						j++;

						while ((j < count) && ((c = dc.searchCache(pba2)) == NULL)) {
							j++; scount++; pba2++;
						}
						j--; //Go back to the last block
						
						//for (l=0; l<scount; l++) {
#ifndef	HOME
							//DiskRead(curTime, DATADISK, 8*(pba1+l), 8);
							DiskRead(curTime, DATADISK, 8*pba1, 8*scount);
#else
							//DiskRead(curTime, DATADISK, pba1+l, 1);
							DiskRead(curTime, DATADISK, pba1, scount);
#endif
						//}
						rwflag = CACHE_ENTRY_CLEAN;
						curTime += (incr * scount);
						// Add missing data block entries to the cache
						for (k=0; k<scount; k++) {
							pba2 = pba1 + k;
#ifndef	HOME
							DATA_BLOCK_READ_CACHE_MISS += 8;
#else
							DATA_BLOCK_READ_CACHE_MISS++;
#endif

#ifndef	HOME
							DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 4096, rtime, wtime, DATADISK, rwflag, NULL, &c);
#else
							DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 512, rtime, wtime, DATADISK, rwflag, NULL, &c);
#endif
						}

						rtime += (incr * scount); 
					}
				}
			}
			ts1 += (incr * s.seg_size);
			m += s.seg_size;
		}

		overhead = clock_stop();
		dtime = disk_stop();

                ind = dtime * 1000 + overhead;
#ifdef  HOME
		update_statistics(r_var, r_mean, r_min, r_max, r_n, seg_size, ind);
#else
		update_statistics(r_var, r_mean, r_min, r_max, r_n, seg_size*8, ind);
#endif

		if (trace->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

		//if ((lastGCTime == 0) || (lastFlushTime == 0)) { 
			// Not yet initialized
			//lastFlushTime = lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			//clock_start();
			//disk_start();
			//m->flushCache();
			//cout << "flushing 30 "  
			//bt.flushCache(METADATADISK);
			//hbt.flushCache(METADATADISK);
			// Dummy writing ...
			//twritecount = dc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			//updateBitMap();
			lastFlushTime = (ts2 / 1000000);

			//overhead = clock_stop();
			//dtime = disk_stop();

			//ind = dtime * 1000 + overhead; // Microseconds
			//w_flush += ind;
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			//cout << "flushing 75% \n";
			//bt.flushCache(METADATADISK);
			//hbt.flushCache(METADATADISK);
			// Dummy writing ...
			//twritecount = dc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			//updateBitMap();
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef	DARCP
		// New interval starting, once per every 10 minutes
		//if (((ts2 / 1000000) - lastDARCTime) > (60000 * 10)) {
		if (((darcInterval + 1) * DARCINTREFCOUNT) >
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
			dc.newInterval();
			darcInterval++;
			//cout << "Darc interval " << darcInterval << endl;
			//lastDARCTime = ts2 / 1000000;
		}

		if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
			//cout << "Darc interval " << darcInterval << endl;
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
	cout <<"\n\nTotal read records processed \t\t\t: " << trace->getCurCount() << endl;
}

int main(int argc, char** argv) {
	//IOTraceReader trace;

	int next_pid = -1;

	int i, j, k, m; 
	int count = 0, scount, op; 
	unsigned int pba1, pba2, pba[MAX_SEG_SIZE], pbastart;
	CacheEntry *c; 
	unsigned long long int rtime, wtime;
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct segment s;
	struct segmenthome s2;
	struct segment4k s1;
	unsigned int twritecount;
	unsigned short rwflag;
	int retval; //, trace15 = 0;
	unsigned long int ind;
	unsigned long int overhead, dtime;
        int tno = 0;
	int seg_size;
	//MDMaps *m;
	struct timespec tval;

#ifdef  DARCP
	unsigned long int DARCINTREFCOUNT;
	unsigned int darcInterval = 0;

	DARCINTREFCOUNT = 512*(DATA_CACHE_SIZE / 512);
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


	//dsim.setf(ios::fixed);

	initCacheMem();

	md = new MDMaps("blockidx.dat", "pbaidx.dat", "hashidx.dat",
			"filehashidx.dat", "buckets.dat", "hashbuckets.dat",
			"pbaref.dat", "fblist.dat", DEDUPFULL);	

	dummy.open(dummyfile.c_str(), ios::in | ios::out | ios::binary);
	openBitMap();
/*
	trace[0] = new IOTraceReader4K();
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
	for (i=0; i<6; i++) {
		if (i == 2)
			trace[i] = new IOTraceReaderHome();
		else    trace[i] = new IOTraceReader4K();

		if ((trace[i]->openTrace(fnames[i], fcount[i],
			fbsize[i]) != 0)) {
			perror("IO Trace opening failure:");
			exit(0);
		}
	}

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t1_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);

	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	while (1) {
		//retval = trace.readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		if (tno != 2) // Not home trace
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
		else 	incr = 2;
		seg_size = s1.seg_size;
		t0_hash += (seg_size * 30);
#else
		if (s2.seg_size > 1)
			incr = (ts2 - ts1) / (s2.seg_size - 1);
		else 	incr = 2;
		seg_size = s2.seg_size;
		t0_hash += (seg_size * 4);
#endif
		curTime = ts1;
		if (incr <= 0) incr = 2;

		clock_start();
		disk_start();

		m = 0;

		// Split and process home requests which are very larger
		while (m < seg_size) {
#ifndef HOME
			s.seg_start = s1.seg_start + m;
			for (i=0; (i<MAX_SEG_SIZE) && ((m + i) < s1.seg_size); i++)
				memcpy(s.seg_hash[i], s1.seg_hash[m+i], 16);
			s.seg_size = i;
#else
			s.seg_start = s2.seg_start + m;
			for (i=0; (i<MAX_SEG_SIZE) && ((m + i) < s2.seg_size); i++)
				memcpy(s.seg_hash[i], s2.seg_hash[m+i], 16);
			s.seg_size = i;
#endif

#ifdef	__DEBUG__
			cout <<  "\t\tseg : " << s.seg_start << " , " << s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

			if (op == OP_READ) dedupReadRequest(&s, md, pba);
			else dedupWriteRequest(&s, md, pba);
		
			// Search for the data blocks segment by segment 
			// in the cache with the representative pba number
			i = 0;
			rtime = wtime = 0;
			if (op == OP_READ) rtime = ts1;
			else wtime = ts1;
			while (i < s.seg_size) {
				pbastart = pba[i];
				count = 1;
				i++;
				while ((i < s.seg_size) && (pba[i] == pba[i-1] + 1)) {
					count++;
					i++;
				}

				// Search for the whole segment?
				for (j=0; j<count; j++) {
					pba1 = pbastart + j;
					c = dc.searchCache(pba1);
					if (c != NULL) {
						//dc.deleteEntry(c);
						if (op == OP_READ) {
#ifndef	HOME
							DATA_BLOCK_READ_CACHE_HIT += 8;
#else
							DATA_BLOCK_READ_CACHE_HIT++;
#endif
							c->ce_refcount++;
							dc.c_rrefs++;
							c->ce_rtime = rtime;
							rtime += incr; 
#ifdef	DARCP
							dc.repositionEntry(c);
#endif
						}
						else {
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
							wtime += incr;
#ifdef	DARCP
							dc.repositionEntry(c);
#endif
						}
						curTime += incr;
					}
					else {
						// Find the sequence of blocks not in 
						// data cache, and issue single request
						scount = 1;
						pba2 = pba1+1;
						j++;

						while ((j < count) && ((c = dc.searchCache(pba2)) == NULL)) {
							j++; scount++; pba2++;
						}
						j--; //Go back to the last block
						
						if (op == OP_READ) {
							//for (l=0; l<scount; l++) {
#ifndef	HOME
								//DiskRead(curTime, DATADISK, 8*(pba1+l), 8);
								DiskRead(curTime, DATADISK, 8*pba1, 8*scount);
#else
								//DiskRead(curTime, DATADISK, pba1+l, 1);
								DiskRead(curTime, DATADISK, pba1, scount);
#endif
							//}
							rwflag = CACHE_ENTRY_CLEAN;
						}
						else {
							rwflag = CACHE_ENTRY_DIRTY;
						}
						curTime += (incr * scount);
						// Add missing data block entries to the cache
						for (k=0; k<scount; k++) {
							pba2 = pba1 + k;
							if (op == OP_READ) {
#ifndef	HOME
								DATA_BLOCK_READ_CACHE_MISS += 8;
#else
								DATA_BLOCK_READ_CACHE_MISS++;
#endif
							}
							else {
#ifndef	HOME
								DATA_BLOCK_WRITE_CACHE_MISS += 8;
#else
								DATA_BLOCK_WRITE_CACHE_MISS++;
#endif
							}

#ifndef	HOME
							DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 4096, rtime, wtime, DATADISK, rwflag, NULL, &c);
#else
							DATA_BLOCK_EFFECTIVE_WRITES += dc.addReplaceEntry(dummy, pba2, NULL, 512, rtime, wtime, DATADISK, rwflag, NULL, &c);
#endif
						}

						if (op == OP_READ) rtime += (incr * scount); 
						else wtime += (incr * scount);
					}
				}
			}
			ts1 += (incr * s.seg_size);
			m += s.seg_size;
		}

		overhead = clock_stop();
		dtime = disk_stop();

                ind = dtime * 1000 + overhead;
#ifdef  HOME
		update_statistics(w_var, w_mean, w_min, w_max, w_n, seg_size, ind);
#else
		update_statistics(w_var, w_mean, w_min, w_max, w_n, seg_size*8, ind);
#endif

		if (trace[tno]->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace[tno]->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

		//if ((lastGCTime == 0) || (lastFlushTime == 0)) { 
			// Not yet initialized
			//lastFlushTime = lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			clock_start();
			disk_start();
			md->flushCache();
			//cout << "flushing 30 "  
			//bt.flushCache(METADATADISK);
			//hbt.flushCache(METADATADISK);
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			updateBitMap();
			lastFlushTime = (ts2 / 1000000);

			overhead = clock_stop();
			dtime = disk_stop();

			ind = dtime * 1000 + overhead; // Microseconds
			w_flush += ind;
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			//cout << "flushing 75% \n";
			//bt.flushCache(METADATADISK);
			//hbt.flushCache(METADATADISK);
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			updateBitMap();
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef	DARCP
		// New interval starting, once per every 10 minutes
		//if (((ts2 / 1000000) - lastDARCTime) > (60000 * 10)) {
		if (((darcInterval + 1) * DARCINTREFCOUNT) >
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
			dc.newInterval();
			darcInterval++;
			//cout << "Darc interval " << darcInterval << endl;
			//lastDARCTime = ts2 / 1000000;
		}

		if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
			//cout << "Darc interval " << darcInterval << endl;
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

	//bt.flushCache(METADATADISK);
	//hbt.flushCache(METADATADISK);
	md->flushCache();
	updateBitMap();

	twritecount = dc.flushCache(dummy, 0, DATADISK);
	DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
	DATA_BLOCK_FLUSH_WRITES += twritecount;

	overhead = clock_stop();
	dtime = disk_stop();

	ind = dtime * 1000 + overhead; // Microseconds
	w_flush += ind;

	cout << "\n\nTotal write records processed \t\t\t" 
		<< trace[tno]->getCurCount() << endl;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t2_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t2_disk0 = DISK_WRITE_TOT_TIME[0];
	t2_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	//==============================

	cout << "\n============ READ Trace ("
		<< frnames[tno] << ") processing =========\n\n";
	processReadTrace(tno, md);

	//===============================

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t3_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t3_disk0 = DISK_READ_TOT_TIME[0];
	t3_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	cout.setf(ios::fixed);
	cout.precision(4);

	cout << "\nt1_clk " << t1_clk << "\nt2_clk " << t2_clk << "\nt3_clk " << t3_clk << "\nt2_disk0 " << t2_disk0 << "\nt3_disk0 " << t3_disk0 << "\nt2_disk1 " << t2_disk1 << "\nt3_disk1 " << t3_disk1 << "\nt0_hash " << t0_hash << endl;

	cout << "Write throughput : " << (((DATA_BLOCK_WRITE_COUNT * 512.0 *
		10000000.0) / (t2_clk - t1_clk + t0_hash + ((t2_disk0 + t2_disk1)*1000)))
		/ (1024 * 1024)) << "MB / sec\n";
	cout << "Read throughput : " << (((DATA_BLOCK_READ_COUNT * 512.0 *
		10000000.0) / (t3_clk - t2_clk +
		((t3_disk0 + t3_disk1 - t2_disk1)*1000)))
		/ (1024 * 1024)) << "MB / sec\n";


	cout << "Working set size : " << WSS << "KB" << endl;
	cout << "Cache sizes used:\n" << "Data Cache size \t\t: "
                << (DATA_CACHE_SIZE / (1024))
                << " KB\n";
       	//	<< "Meta data cache size \t\t: "
                //<< ((HASHINDEX_CACHE_SIZE + BLOCKINDEX_CACHE_SIZE + 
			//FP_CACHE_SIZE + PBAREF_CACHE_SIZE +
			//LBA2PBA_CACHE_SIZE) / (1024))
		//<< " KB\n";

	//cout << "FP Cache Hit count \t\t: " << FP_CACHE_HIT << endl
		//<< "FP Cache Miss count \t\t: " << FP_CACHE_MISS << endl
		//<< "LBA to PBA Cache Hit Count \t: " << LBA2PBA_CACHE_HIT << endl
		//<< "LBA to PBA Cache Miss Count \t: " << LBA2PBA_CACHE_MISS << endl;

	//cout << "PBA Reference count cache hit \t: " << PBAREF_CACHE_HIT << endl
		//<< "PBA Reference count cache miss \t: " << PBAREF_CACHE_MISS  << endl;
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
		<< "DATA READ CACHE MISS \t\t: " << DATA_BLOCK_READ_CACHE_MISS << endl
		<< "DATA WRITE COUNT \t\t: " << DATA_BLOCK_WRITE_COUNT << endl
		<< "DATA WRITE CACHE HITS \t\t: " << DATA_BLOCK_WRITE_CACHE_HIT << endl
		<< "DATA WRITE CACHE MISS \t\t: " << DATA_BLOCK_WRITE_CACHE_MISS << endl
		<< "DATA BLOCK EFFECTIVE READS \t: " << (DATA_BLOCK_READ_COUNT - DATA_BLOCK_READ_CACHE_HIT) << endl
		<< "DATA BLOCK EFFECTIVE WRITES \t: " << DATA_BLOCK_EFFECTIVE_WRITES << endl
		<< "DATA BLOCK FLUSH WRITES \t: " << DATA_BLOCK_FLUSH_WRITES << endl;
		

	//cout << trace[tno]->getCurCount() << "  number of records processed" << endl;
	cout << "\n\nTotal Blocks \t\t\t: " << md->bt->keycount << endl
		<< "Unique blocks identified \t: " << md->hbt->keycount << endl
		<< "Unique blocks identified (Allocated) \t: " << UNIQUE_BLOCK_COUNT << endl;

	md->displayStatistics();

	display_io_response();

	//cout << "\n\nHash BTree index\n";
	//hbt.displayStatistics();

	//cout << "\n\nLBA BTree index\n";
	//bt.displayStatistics();

	cout << "\n\nData cache statistics \n";
	dc.displayStatistics();


	//cout << "\n\nEFFECTIVE META DATA OPERATION COUNTS\n\n";
	//cout << "BT SEARCH \t\t\t: " << bt.BTSEARCH  << endl
		//<< "BT INSERT \t\t\t: " << bt.BTINSERT << endl
		//<< "BT DELETE \t\t\t: " << bt.BTDELETE << endl
		//<< "BT UPDATE \t\t\t: " << bt.BTUPDATE << endl
		//<< "HBT SEARCH \t\t\t: " << hbt.BTSEARCH  << endl
		//<< "HBT INSERT \t\t\t: " << hbt.BTINSERT << endl
		//<< "HBT DELETE \t\t\t: " << hbt.BTDELETE << endl
		//<< "HBT UPDATE \t\t\t: " << hbt.BTUPDATE  << endl
		//<< "PBREF SEARCH \t\t\t: " << PBREFSEARCH << endl
		//<< "PBREF INSERT \t\t\t: " << PBREFINSERT << endl
		//<< "PBREF DELETE \t\t\t: " << PBREFDELETE << endl
		//<< "PBREF UPDATE \t\t\t: " << PBREFUPDATE << endl
		//<< "Total SEARCH \t\t\t\t: " << bt.BTSEARCH + hbt.BTSEARCH + PBREFSEARCH  << endl
		//<< "Total INSERT \t\t\t\t: " << bt.BTINSERT + hbt.BTINSERT + PBREFINSERT  << endl
		//<< "Total DELETE \t\t\t\t: " << bt.BTDELETE + hbt.BTDELETE + PBREFDELETE  << endl
		//<< "Total UPDATE \t\t\t\t: " << bt.BTUPDATE + hbt.BTUPDATE + PBREFUPDATE << endl << endl;

/*
	float rr_ratio, rw_ratio, wr_ratio, ww_ratio;
	float err_ratio, erw_ratio, ewr_ratio, eww_ratio;
	//float READ_RESPONSE_TIME, WRITE_RESPONSE_TIME;
	//float EFFECTIVE_READ_RESPONSE_TIME, EFFECTIVE_WRITE_RESPONSE_TIME;

	rr_ratio = ((float)(hbt.BT_BTNODE_READCOUNT_READ +
			bt.BT_BTNODE_READCOUNT_READ)) / DATA_BLOCK_READ_COUNT;

	rw_ratio = ((float)(hbt.BT_BTNODE_WRITECOUNT_READ +
			bt.BT_BTNODE_WRITECOUNT_READ)) / DATA_BLOCK_READ_COUNT;

	wr_ratio = ((float)(hbt.BT_BTNODE_READCOUNT_WRITE +
			bt.BT_BTNODE_READCOUNT_WRITE)) / DATA_BLOCK_WRITE_COUNT;

	ww_ratio = ((float)(hbt.BT_BTNODE_WRITECOUNT_WRITE +
			bt.BT_BTNODE_WRITECOUNT_WRITE)) / DATA_BLOCK_WRITE_COUNT;

	err_ratio = ((float)(((hbt.BT_BTNODE_READCOUNT -
			hbt.BT_BTNODE_READ_CACHE_HIT) +
			(bt.BT_BTNODE_READCOUNT -
			bt.BT_BTNODE_READ_CACHE_HIT)) * rr_ratio)) /
			((rr_ratio + wr_ratio) * DATA_BLOCK_READ_COUNT);
	ewr_ratio = ((float)(((hbt.BT_BTNODE_READCOUNT -
			hbt.BT_BTNODE_READ_CACHE_HIT) +
			(bt.BT_BTNODE_READCOUNT -
			bt.BT_BTNODE_READ_CACHE_HIT)) * wr_ratio)) /
			((rr_ratio + wr_ratio) * DATA_BLOCK_WRITE_COUNT);
	erw_ratio = (((float)(hbt.BT_BTNODE_EFFECTIVE_WRITES +
			bt.BT_BTNODE_EFFECTIVE_WRITES)) * rw_ratio) /
			((rw_ratio + ww_ratio) * DATA_BLOCK_READ_COUNT);

	eww_ratio = (((float)(hbt.BT_BTNODE_EFFECTIVE_WRITES +
			bt.BT_BTNODE_EFFECTIVE_WRITES)) * ww_ratio) /
			((rw_ratio + ww_ratio) * DATA_BLOCK_WRITE_COUNT);

	cout << "Meta data reads " << rr_ratio << ", writes " << rw_ratio << " / Data block read\n\n";
	cout << "Meta data reads " << wr_ratio << ", writes " << ww_ratio << " / Data block write\n\n";
	//cout << "Effective Meta data reads " << err_ratio << ", writes " << erw_ratio << " / Data block read\n\n";
	//cout << "Effective Meta data reads " << ewr_ratio << ", writes " << eww_ratio << " / Data block write\n\n";

	rr_ratio = ((float)(hbt.BT_BTNODE_READCOUNT_READ +
			bt.BT_BTNODE_READCOUNT_READ +
			PBREF_READCOUNT_READ)) / DATA_BLOCK_READ_COUNT;

	rw_ratio = ((float)(hbt.BT_BTNODE_WRITECOUNT_READ +
			bt.BT_BTNODE_WRITECOUNT_READ +
			PBREF_WRITECOUNT_READ)) / DATA_BLOCK_READ_COUNT;

	wr_ratio = ((float)(hbt.BT_BTNODE_READCOUNT_WRITE +
			bt.BT_BTNODE_READCOUNT_WRITE +
			PBREF_READCOUNT_WRITE)) / DATA_BLOCK_WRITE_COUNT;

	ww_ratio = ((float)(hbt.BT_BTNODE_WRITECOUNT_WRITE +
			bt.BT_BTNODE_WRITECOUNT_WRITE +
			PBREF_WRITECOUNT_WRITE)) / DATA_BLOCK_WRITE_COUNT;

	err_ratio = ((float)(((hbt.BT_BTNODE_READCOUNT -
			hbt.BT_BTNODE_READ_CACHE_HIT) +
			(bt.BT_BTNODE_READCOUNT -
			bt.BT_BTNODE_READ_CACHE_HIT) +
			PBREF_READCOUNT_READ) * rr_ratio)) /
			((rr_ratio + wr_ratio) * DATA_BLOCK_READ_COUNT);
	ewr_ratio = ((float)(((hbt.BT_BTNODE_READCOUNT -
			hbt.BT_BTNODE_READ_CACHE_HIT) +
			(bt.BT_BTNODE_READCOUNT -
			bt.BT_BTNODE_READ_CACHE_HIT) +
			PBREF_READCOUNT_WRITE) * wr_ratio)) /
			((rr_ratio + wr_ratio) * DATA_BLOCK_WRITE_COUNT);
	erw_ratio = (((float)(hbt.BT_BTNODE_EFFECTIVE_WRITES +
			bt.BT_BTNODE_EFFECTIVE_WRITES) +
				PBREF_WRITECOUNT_READ) * rw_ratio) /
			((rw_ratio + ww_ratio) * DATA_BLOCK_READ_COUNT);

	eww_ratio = (((float)(hbt.BT_BTNODE_EFFECTIVE_WRITES +
			bt.BT_BTNODE_EFFECTIVE_WRITES) +
				PBREF_WRITECOUNT_WRITE) * ww_ratio) /
			((rw_ratio + ww_ratio) * DATA_BLOCK_WRITE_COUNT);

	cout <<"\n\nIncluding PBREF\n";
	cout << "Meta data reads " << rr_ratio << ", writes " << rw_ratio << " / Data block read\n\n";
	cout << "Meta data reads " << wr_ratio << ", writes " << ww_ratio << " / Data block write\n\n";
	cout << "Effective Meta data reads " << err_ratio << ", writes " << erw_ratio << " / Data block read\n\n";
	cout << "Effective Meta data reads " << ewr_ratio << ", writes " << eww_ratio << " / Data block write\n\n";

	//cout << "\n\nExcluding FLUSH Writes\n\n";

	erw_ratio = (((float)(hbt.BT_BTNODE_EFFECTIVE_WRITES -
			hbt.BT_BTNODE_FLUSH_WRITES +
			bt.BT_BTNODE_EFFECTIVE_WRITES -
			bt.BT_BTNODE_FLUSH_WRITES)) * rw_ratio) /
			((rw_ratio + ww_ratio) * DATA_BLOCK_READ_COUNT);

	eww_ratio = (((float)(hbt.BT_BTNODE_EFFECTIVE_WRITES -
			hbt.BT_BTNODE_FLUSH_WRITES +
			bt.BT_BTNODE_EFFECTIVE_WRITES -
			bt.BT_BTNODE_FLUSH_WRITES)) * ww_ratio) /
			((rw_ratio + ww_ratio) * DATA_BLOCK_WRITE_COUNT);

	//cout << "Meta data reads " << rr_ratio << ", writes " << rw_ratio << " / Data block read\n\n";
	//cout << "Meta data reads " << wr_ratio << ", writes " << ww_ratio << " / Data block write\n\n";

	//cout << "Effective Meta data reads " << err_ratio << ", writes " << erw_ratio << " / Data block read\n\n";
	//cout << "Effective Meta data reads " << ewr_ratio << ", writes " << eww_ratio << " / Data block write\n\n";
*/
	cout << "SEGCOUNT3 : " << SEGCOUNT3 << " SEGLENGTH3 : " << SEGLENGTH3 << " AVG SEGMENT LENGTH : " << (SEGLENGTH3 / SEGCOUNT3) << endl;
	//updateBitMap();
	//dsim.close();
	trace[tno]->closeTrace();	

	return 0;
}
