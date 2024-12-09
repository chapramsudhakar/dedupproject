// Hybrid Deduplication main function

#include <iostream>
#include <fstream>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#include "dedupconfig.h"
#include "dedupn.h"
//#include "cache.h"
#include "cachemem.h"
//#include "bucket.h"
//#include "hbucket.h"
#include "bitmap.h"
//#include "darc.h"
//#include "lru.h"
#include "metadata.h"
#include "iotracereader.h"
#include "util.h"
#include "iotraces.h"
#include "filerecipe.h"

void md5(unsigned char *buf, unsigned int nbytes, unsigned char hash[16]);

void cacheMemStats(void);

string dcstr = "datacache";
//#ifdef	DARCP
//#ifdef	HOME
//DARC dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE, 512);
//#else
//DARC dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
//#endif
//#else
//LRU dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE);
//#endif

IOTraceReader *trace[10];

string dummyfile="/dev/null";
fstream dummy;

unsigned long int prevblock[2];

unsigned int reccount100000 = 1;
unsigned long long int curTime;

/************************************************/

//unsigned long long int DATA_BLOCK_READ_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT_LDC = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT_INDIRECT = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS_LDC = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS_INDIRECT = 0;
unsigned long long int DATA_BLOCK_READ_COUNT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_MISS = 0;
unsigned long long int DATA_BLOCK_WRITE_COUNT = 0;
//unsigned long long int DATA_BLOCK_EFFECTIVE_WRITES = 0;
//unsigned long long int DATA_BLOCK_FLUSH_WRITES = 0;
//unsigned long long int UNIQUE_BLOCK_COUNT = 0;

//unsigned long long int DISK_READ_COUNT[2] = {0};
//unsigned long long int DISK_READ_BCOUNT[2] = {0};
//unsigned long long int DISK_WRITE_COUNT[2] = {0};
//unsigned long long int DISK_WRITE_BCOUNT[2] = {0};
//double DISK_READ_TOT_TIME[2] = {0};             // Time in mille seconds
//double DISK_WRITE_TOT_TIME[2] = {0};
//double DISK_READ_MIN_TIME[2] = {10000}; // Time in mille seconds
//double DISK_READ_MAX_TIME[2] = {0};
//double DISK_WRITE_MIN_TIME[2] = {10000};        // Time in mille seconds
//double DISK_WRITE_MAX_TIME[2] = {0};
//unsigned long int DISK_READ_REQ_COUNT[1028][1000];
//unsigned long int DISK_WRITE_REQ_COUNT[1028][1000];
//unsigned long int DISK_IWRITE_REQ_COUNT[1028][1000];
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

//double SEGCOUNT3 = 0.0;
//double SEGLENGTH3 = 0.0;

int OPERATION;

//MDMaps *md;

#ifdef	WEB
#define	MAX_SEG_SIZE_USED	(MAX_SEG_SIZE / 2)
#else
#define	MAX_SEG_SIZE_USED	MAX_SEG_SIZE
#endif

double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

double t1_disk0 = 0, t1_disk1 = 0, t2_disk0 = 0, t2_disk1 = 0;
double t3_disk0 = 0, t3_disk1 = 0;
double t1_clk = 0, t2_clk = 0, t3_clk = 0;
double t0_hash = 0, t0_dcomm = 0, t0_mdcomm = 0, t0_rcomm = 0;
/*
 * sn^2 = (n-2)/(n-1)*sn-1^2 + 1/n*(xn - mean(xn-1)^2
 * mean(xn) = ((n-1)*mean(xn-1) + xn) / n;
 */

unsigned long int CLIENT_CLOCK_PREV = 0, CLIENT_CLOCK_MICRO = 0;

//struct segment s[64];
//unsigned char hashes[256][16];
unsigned char lcounters[8192];

StorageNode *snodes[64];
IIIBTree *frcp;
FBList	*cfbl;

void update_statistics(double var[], double mean[],
	double minimum[], double maximum[],
	unsigned long int  N[], int seglen, unsigned int microsec) {
	unsigned long int n, i, count;

	count = ((seglen + 1023) / 1024);
	seglen /= count;
	microsec /= count;	

	for (i=0; i<count; i++) {
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
}


void countSegmentLength(StorageNode *sn, unsigned int pba[], unsigned int seg_size) {
	unsigned int i;
	int diff;

	sn->SEGLENGTH3 += seg_size;

	// Already not existing
	i = 1;
	while (i<seg_size) {

		diff = pba[i] - pba[i-1];

		if (abs(diff) > 2) {
			sn->SEGCOUNT3++;
		}
		i++;
	}

	sn->SEGCOUNT3++;	// Last segment
}


/*
void readSegmentDC(unsigned int pba, unsigned int count, unsigned long long int ts, unsigned long long int incr) {
	unsigned int j, k;
	unsigned int pba1, pba2;
	unsigned int rtime = ts;
	unsigned int scount;
	CacheEntry *c;
	unsigned short cflags;
	int wcount;

#ifdef	__DEBUG__
	cout << "readSegmentDC pba " << pba << " count " << count << endl;
#endif
	// Search for the whole segment?
	for (j=0; j<count; j++) {
		pba1 = pba + j;
		c = dc.searchCache(pba1);
		if (c != NULL) {
#ifndef	HOME
			DATA_BLOCK_READ_CACHE_HIT += 8;
#else
			DATA_BLOCK_READ_CACHE_HIT++;
#endif
			c->ce_refcount++;
			c->ce_rtime = rtime;
			dc.c_rrefs++;
			rtime += incr;
			curTime += incr;
#ifdef	DARCP
			dc.repositionEntry(c);
#endif
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
		
#ifndef	HOME
			DiskRead(curTime, DATADISK, 8*pba1, 8*scount);
			DATA_BLOCK_READ_CACHE_MISS += (8 * scount);
#else
			DiskRead(curTime, DATADISK, pba1, scount);
			DATA_BLOCK_READ_CACHE_MISS += scount;
#endif
			cflags = CACHE_ENTRY_CLEAN;
			curTime += (incr * scount);

			// Add missing data block entries to the cache
			for (k=0; k<scount; k++) {
				pba2 = pba1 + k;
#ifndef	HOME
				wcount = dc.addReplaceEntry(dummy, pba2, NULL, 4096, rtime, 0, DATADISK, cflags, NULL, &c);
#else
				wcount = dc.addReplaceEntry(dummy, pba2, NULL, 512, rtime, 0, DATADISK, cflags, NULL, &c);
#endif
				//cout << "pba " << pba2 
					//<< " dc.count " << dc.getCount() 
					//<< " wcount " << wcount << endl;	
				DATA_BLOCK_EFFECTIVE_WRITES += wcount;
			}

			rtime += (incr * scount);
		}
	}
}

void writeSegmentDC(unsigned int pba, unsigned int count, unsigned long long int ts, unsigned long long int incr) {
	unsigned int j;
	unsigned int pba1;
	unsigned int wtime = ts;
	CacheEntry *c;
	unsigned short cflags;
	int wcount;

	// Search for the whole segment?
	for (j=0; j<count; j++) {
		pba1 = pba + j;
		c = dc.searchCache(pba1);
		if (c != NULL) {
#ifndef HOME
			DATA_BLOCK_WRITE_CACHE_HIT += 8;
#else
			DATA_BLOCK_WRITE_CACHE_HIT++;
#endif
			c->ce_refcount++;
			c->ce_wrefcount++;
			dc.c_wrefs++;
			c->ce_wtime = wtime;
			if ((c->ce_flags & CACHE_ENTRY_DIRTY) == 0) {
				c->ce_flags |= CACHE_ENTRY_DIRTY;
				dc.c_dcount++;
			}
#ifdef  DARCP
			dc.repositionEntry(c);
#endif
		}
		else {
			cflags = CACHE_ENTRY_DIRTY;
#ifndef HOME
			DATA_BLOCK_WRITE_CACHE_MISS += 8;
#else
			DATA_BLOCK_WRITE_CACHE_MISS++;
#endif

#ifndef HOME
			wcount = dc.addReplaceEntry(dummy, pba1, NULL, 4096, 0, wtime, DATADISK, cflags, NULL, &c);
#else
			wcount = dc.addReplaceEntry(dummy, pba1, NULL, 512, 0, wtime, DATADISK, cflags, NULL, &c);
#endif
			DATA_BLOCK_EFFECTIVE_WRITES += wcount;
		}
		wtime += incr;
		curTime += incr;
	}
}
*/
/*
void readFileData(unsigned int pba[], unsigned int seg_size, unsigned long long int ts, unsigned long long int incr) {
	unsigned long long int rtime;
	unsigned int i;
	unsigned int pbastart, count;

	// Search for the data blocks segment by segment in the cache
	// with the representative pba number
	i = 0;
	rtime = ts;

	while (i < seg_size) {
		pbastart = pba[i];
		count = 1;
		i++;

		while ((i < seg_size) && (pba[i] == (pba[i-1] + 1))) {
			count++;
			i++;
		}

		readSegmentDC(pbastart, count, rtime, incr);
		rtime += (incr * count);
	}
}

void writeFileData(unsigned int pba[], unsigned int seg_size, unsigned long long int ts, unsigned long long int incr) {
	unsigned long long int wtime;
	unsigned int i;
	unsigned int pbastart, count;

	// Search for the data blocks segment by segment in the cache
	// with the representative pba number
	i = 0;
	wtime = ts;

	while (i < seg_size) {
		pbastart = pba[i];
		count = 1;
		i++;

		while ((i < seg_size) && (pba[i] == (pba[i-1] + 1))) {
			count++;
			i++;
		}

		writeSegmentDC(pbastart, count, wtime, incr);
		wtime += (incr * count);
	}
}

// Get lba to pba map values from IBTree
void getLba2PbaMaps(unsigned int lbastart, unsigned int nb,
	IBTree *ibt, unsigned int pba[]) {
	unsigned long long int lba;
	unsigned int i;

	lba = lbastart;

	for (i=0; i<nb; i++) {
		ibt->searchValWrap(lba, &pba[i]);
		lba++;
	}
}
*/

// Process read file 
void processReadFile(struct FileBlocks *fb, unsigned int pba[]) {
	unsigned int clba;
	unsigned int i;
	unsigned int keylba, nblocks, fbentry, n, entlba, nextfbe; //, curlba;
	unsigned int count, comp = 0;
	unsigned long v;

	frcp->searchRangeWrap(fb->fb_lbastart, &keylba, &nblocks, &fbentry);
	if (keylba == 0) {
		cout << "Process read file :" << fb->fb_lbastart << " file recipe not found\n";
	}
	else {
		clba = fb->fb_lbastart;	
		cfbl->fbeGetEntry(fbentry, &entlba, &n, &nblocks, &nextfbe);
		count = (nblocks < fb->fb_nb) ? nblocks : fb->fb_nb;
		while (comp < fb->fb_nb) {
			v = clock_get();
			// Find logical to physical block maps on node n
			for (i=0; i<count; i++) 
				snodes[n]->md->bt->searchValWrap(clba+i, &pba[comp+i]);

			snodes[n]->RSEGCOUNT3 += 1;
			snodes[n]->RSEGLENGTH3 += count;
			snodes[n]->t_rcumulative += (clock_get() - v);

			comp += count;
			clba += count;
			fbentry = nextfbe;
			if (comp < fb->fb_nb) {
				if (fbentry != 0)
					cfbl->fbeGetEntry(fbentry, &entlba, &n, &nblocks, &nextfbe);
				else {
					frcp->searchRangeWrap(clba, &keylba, &nblocks, &fbentry);

					if (keylba == 0) break;
					cfbl->fbeGetEntry(fbentry, &entlba, &n, &nblocks, &nextfbe);
				}
				count = (nblocks < (fb->fb_nb-comp)) ? nblocks : (fb->fb_nb-comp);
			}
		}
		t0_rcomm += (((fb->fb_nb * 4096 + 1499)/1500) * PKTCOMMTIME);
		if (comp < fb->fb_nb)
			cout << "Process read file :" << fb->fb_lbastart << " file block list is partial- comp " << comp << " fbnb " << fb->fb_nb << " entlba " << entlba << " n " << n << " nblocks " << nblocks << endl;
	}
}

void processReadTrace(int tno) {
	IOTraceReader *rtrace;
	//int op; 
	unsigned int pbasize = 0;
	unsigned int *pba = NULL;
	//unsigned int lastFlushTime = 0, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct FileBlocks fb;
	//int count;
	int retval; 
	//unsigned int ind;
	//unsigned long dtime, overhead;

	reccount100000 = 1;
	// Open the read operations trace
	if (tno == 2)
		rtrace = new IOTraceReaderHome();
	else 	rtrace = new IOTraceReader4K();

	if (rtrace->openTrace(frnames[tno], fbsize[tno]) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}

	fb.fb_hlist = NULL;

	while (1) {
		retval = rtrace->readFile(&fb);

		if (retval == 0) break;	// End of the trace is reached

		ts1 = fb.fb_ts1;
		ts2 = fb.fb_ts2;
		//op = fb.fb_rw;

		READ_REQ_COUNT++;
		OPERATION = OPERATION_READ;
		DATA_BLOCK_READ_COUNT += fb.fb_nb;

		fb.fb_nb /= 8;
		fb.fb_lbastart /= 8;

		if (fb.fb_nb > 1)
			incr = (ts2 - ts1) / (fb.fb_nb - 1);
		else	incr = 2;

		curTime = ts1;
		if (incr <= 0) incr = 2;

		if (pbasize < fb.fb_nb) {
			if (pba != NULL) free(pba);
			pbasize = ((fb.fb_nb + 255)/256) * 256;
			pba = (unsigned int *)malloc(pbasize * sizeof(unsigned int));
			assert(pba != NULL);
		}

		//clock_start();
		//disk_start();

		processReadFile(&fb, pba);

		//readFileData(pba, fb.fb_nb);

		//overhead = clock_stop();
		//dtime = disk_stop();

		//ind = dtime * 1000 + overhead;
//#ifdef  HOME
		//update_statistics(r_var, r_mean, r_min, r_max, r_n, fb.fb_nb, ind);
//#else
		//update_statistics(r_var, r_mean, r_min, r_max, r_n, fb.fb_nb*8, ind);
//#endif

		if (rtrace->getCurCount() >= (reccount100000 * 1000000)) {
			cout << rtrace->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

#ifdef  DARCP
                if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
                        //dc.decayWeights();
                        //ldc.decayWeights();
                        lastDecayTime = ts2 / 1000000;
                }

                // New interval starting, once per every 10 minutes
		if (((darcInterval + 1) * DARCINTREFCOUNT) > 
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
                        dc.newInterval();
			darcInterval++;
                }
#endif
	}
	cout <<"\n\nTotal read records processed \t\t\t: " << rtrace->getCurCount() << endl;
}



void updateCounters(unsigned char counter1[], unsigned char counter2[],
		unsigned char counter3[]) {
	unsigned int i;

	for (i=0; i<8192; i++)
		counter3[i] = (counter1[i] > counter2[i]) ? counter1[i] : counter2[i];
	return;
}


double estimateCardinality(unsigned char counter[]) {
	//unsigned int i, j, n, found;
	unsigned int j;
	double v = 0.0, R;

	for (j=0; j<8192; j++) v = v + counter[j];

	R = v/8192;

	// return Alpha_m . m . 2^R;
	return (0.721205 * 8192 * pow(2.0, R));
}

void computeCounters(unsigned char counter[], struct HashList *h, int nh) {
	int i, j, r, byteno;
	struct HashList *h1;

	h1 = h;
	for (i=0; i<nh; i++) {
		// extract 13 - LSB bits (counter number j)
		j = ((h1->hl_hash[1] & 0x1F) << 8) | h1->hl_hash[0];
		r = 0;
		byteno = 1;
		while ((h1->hl_hash[byteno] & (1 << ((r + 5) % 8))) == 0) {
			r++;
			if (((r+5) % 8) == 0) byteno++;
			if (byteno == 16) break;
		}
		if (counter[j] < r) counter[j] = r;

		h1 = h1->hl_next;
	}
}

void processSegment(StorageNode *sn, struct FileBlocks *fb,
	unsigned int pba[]) {
	unsigned int clba;
	HashList *hl1;
	unsigned int i, j, bn;
	unsigned char *mhash;
	unsigned int count;
	unsigned int bnonew, seg_pba;
	unsigned int bpba[MAX_DEDUP_SEG_SIZE], bbnos[MAX_DEDUP_SEG_SIZE];
	unsigned int brefcounts[MAX_DEDUP_SEG_SIZE];
	unsigned int ubcount = 0;

	//clock_add(sn, fb->fb_nb * 30);
	//sn->t0_hash += (fb->fb_nb * 30);

	// Divide the file into MAX_SEG_SIZE segments
	hl1 = fb->fb_hlist;
	clba = fb->fb_lbastart;
	for (i=0; i<fb->fb_nb; i=i+MAX_DEDUP_SEG_SIZE) {
		// Find min hash for the next segment
		count = ((i+MAX_DEDUP_SEG_SIZE) <= fb->fb_nb) ? MAX_DEDUP_SEG_SIZE
			: (fb->fb_nb - i);
		mhash = minHash3(hl1, count);

		// Locate the similar bin/bucket
		bn = sn->md->bkt->findBucket(mhash);

		if (bn == 0) {
			// New bucket is to be created
			// Add this segment to new bucket
			bnonew = sn->md->bkt->addNewBucket(mhash);
			assert(bnonew > 0);

			// Allocate new blocks
			seg_pba = sn->bmap->allocBlocks(count);
			assert(seg_pba > 0);
			sn->UNIQUE_BLOCK_COUNT += count;
			ubcount += count;

			// Add lba-pba-bno new entries
			for (j=0; j<count; j++) {
				pba[i+j] = seg_pba + j;
				sn->md->bkt->insertValWrap(bnonew+HASH_BUCK_COUNT, pba[i+j], hl1->hl_hash, 1);
				sn->md->bt->insertValWrap(clba+j, pba[i+j]);
				sn->md->pb->insertValWrap(pba[i+j], bnonew+HASH_BUCK_COUNT);
				hl1 = hl1->hl_next;
			}
		}
		else {
			// Deduplicate the segment and store the unique blocks
			// Update metadata maps
			for (j=0; j<count; j++)
				bpba[j] = bbnos[j] = 0;
			sn->md->bkt->searchHashesWrap(bn+HASH_BUCK_COUNT, hl1, count, bpba, bbnos, brefcounts);
			// First add missing hashes 
			for (j=0; j<count; j++) {
				if (bpba[j] == 0) {
					// Hash not found in the bucket
					pba[i+j] = sn->bmap->allocBlocks(1);
					assert(pba[i+j] > 0);
					sn->UNIQUE_BLOCK_COUNT ++;
					ubcount++;
				}
			}

			for (j=0; j<count; j++) {
				if (bpba[j] != 0) {
					sn->md->bkt->incrementRefValWrap(bbnos[j], bpba[j]);
					pba[i+j] = bpba[j];
				}
				else {
					sn->md->bkt->insertValWrap(bn+HASH_BUCK_COUNT, pba[i+j], hl1->hl_hash, 1);
					sn->md->pb->insertValWrap(pba[i+j], bn+HASH_BUCK_COUNT);
				}
				sn->md->bt->insertValWrap(clba+j, pba[i+j]);
				hl1 = hl1->hl_next;
			}
		}
		clba += count;
	}

	countSegmentLength(sn, pba, fb->fb_nb);
	sn->ublocks += ubcount;

	t0_dcomm += (((ubcount * 4096 + 1499) / 1500) * PKTCOMMTIME);
}

double findAverageUsedBlocks(void) {
	double ub = 0;
	int i;

	for (i=0; i<SNCOUNT; i++)
		ub += snodes[i]->ublocks;

	if (ub < SNCOUNT) ub = SNCOUNT;
	return (ub / SNCOUNT);
}


void processHUFile(struct FileBlocks *fb) {
	struct HashList *h;
	unsigned int i, j, n, count, ucount, selNode;
	struct FileBlocks fb1;
	double z, minz, maxz;
	unsigned int fbnext, fbent, fbentstart;
	unsigned int pba[MAX_ROUTING_SIZE];
	unsigned long v;
	double avgUsedBlocks;
	unsigned long long int incr;

	if (fb->fb_nb > 1)
		incr = (fb->fb_ts2 - fb->fb_ts1) / (fb->fb_nb - 1);
	else	incr = 2;

	if (incr <= 0) incr = 2;



	// Create a new file segments list
	fbentstart = fbent = cfbl->getFreeFbe();
	fbnext = 0;

	h = fb->fb_hlist;
	for (i=0; i<fb->fb_nb; i=i+MAX_ROUTING_SIZE) {
		count = ((i+MAX_ROUTING_SIZE) <= fb->fb_nb) ? MAX_ROUTING_SIZE : (fb->fb_nb - i);
		for (j=0; j<8192; j++) lcounters[j] = 0;

		computeCounters(lcounters, h, count);
		//minz = 0xFFFFFFFF;
		maxz = 0;
		selNode = 0;
		avgUsedBlocks = findAverageUsedBlocks();	
		for (n=0; n<SNCOUNT; n++) {
			// Estimate the number of unique blocks
			v = clock_get();
			ucount = snodes[n]->estimateUniqueBlockCount(lcounters, count);
			// Select the node with minimum z
			//z = ucount * snodes[n]->ublocks / avgUsedBlocks;
			//if (z < minz) {
				//selNode = n;
				//minz = z;
			//}

			// Select the node with maximum z
			z = (snodes[n]->ublocks > avgUsedBlocks) ? 1.0 :
				(((snodes[n]->ublocks != 0) ? snodes[n]->ublocks : 1) / avgUsedBlocks);
			z = ucount / z;
			if (z > maxz) {
				selNode = n;
				maxz = z;
			}	
			snodes[n]->t_cumulative += (clock_get() - v);
		}

		// Prepare <=1MB size file segment, 
		// deduplicate it and store on selNode
		fb1.fb_ts1 = fb->fb_ts1 + i*incr;
		fb1.fb_ts2 = fb1.fb_ts1 + (count-1) * incr;
		fb1.fb_lbastart = fb->fb_lbastart + i;
		fb1.fb_dev = fb->fb_dev;
		fb1.fb_nb = count;
		fb1.fb_rw = fb->fb_rw;
		fb1.fb_hlist = h;

		v = clock_get();
		processSegment(snodes[selNode], &fb1, pba);
		snodes[selNode]->t_cumulative += (clock_get() - v);
		updateCounters(snodes[selNode]->counters, lcounters, snodes[selNode]->counters);

		// Add a new file recipe entry and link to
		// last entry, remember new last entry
		if ((i + count) < fb->fb_nb)  {
			// There are more super chunks 
			fbnext = cfbl->getFreeFbe();
		}
		else	fbnext = 0;

		// Save current super chunk entry
		cfbl->fbePutEntry(fbent, fb1.fb_lbastart, selNode, count, fbnext);
		fbent = fbnext;

		// Skip count number of hashes
		for (j=0; j<count; j++)
			h = h->hl_next;

		t0_mdcomm += ((8192 + 1499) / 1500 + 1) * PKTCOMMTIME;
	}

	// Add the file recipe entry
	frcp->insertValWrap(fb->fb_lbastart, fb->fb_nb, fbentstart);
}

void addNewFile(int n, unsigned char filehash[], unsigned int lba, 
		unsigned int nblocks) {
	unsigned int fbpba, fbnb, fbnext, fbent, fbentstart;
	unsigned int i, k, nb, stpba;
	unsigned int clntfbestart;
	//unsigned long v;
	StorageNode *sn = snodes[n];

	// Not present, so add the entry
	k = 0;
	clntfbestart = cfbl->getFreeFbe();
	fbentstart = fbent = sn->md->fbl->getFreeFbe();
	fbpba = 0;
	fbnb = 0;
	fbnext = 0;

	while  (k<nblocks) {
		nb = (k+16 <= nblocks) ? 16 : (nblocks - k);
		stpba = sn->bmap->allocBlocks(nb);
		assert(stpba > 0);

		if (fbpba == 0) {
			// First segment
			fbpba = stpba;
			fbnb = nb;
		}
		else if ((fbpba + fbnb) == stpba) {
			// Adjacent to current segment, merge
			fbnb += nb;
		}
		else {
			// New segment start
			fbnext = sn->md->fbl->getFreeFbe();

			// Save old segment
			sn->md->fbl->fbePutEntry(fbent, fbpba, n, fbnb, fbnext);

			fbent = fbnext;
			fbpba = stpba;
			fbnb = nb;
			fbnext = 0;
		}

		for (i=0; i<nb; i++) 
			sn->md->bt->insertValWrap(lba+k+i, stpba+i);
		
		k += nb;
	}

	// Save the last segment
	sn->md->fbl->fbePutEntry(fbent, fbpba, n, fbnb, fbnext);
	sn->md->fhbt->insertValWrap(filehash, fbentstart, nblocks, 1);

	sn->UNIQUE_BLOCK_COUNT += nblocks;
	cfbl->fbePutEntry(clntfbestart, lba, n, nblocks, 0);
	frcp->insertValWrap(lba, nblocks, clntfbestart);
}

void addLbaMap(unsigned int n, unsigned int lba, unsigned int nb, 
		unsigned int stent) {
	unsigned int i, k, node;
	unsigned int fbpba, fbnb, fbnext, fbe;
	unsigned int clntfbestart;
	StorageNode *sn;

	sn = snodes[n];

	// Add an entry to the local cfbl table and frcp
	clntfbestart = cfbl->getFreeFbe();
	cfbl->fbePutEntry(clntfbestart, lba, n, nb, 0);
	frcp->insertValWrap(lba, nb, clntfbestart);

	// Add lba2pba maps on storage node n

	k = 0;
	fbe = stent;
	while (fbe != 0) {
		sn->md->fbl->fbeGetEntry(fbe, &fbpba, &node, &fbnb, &fbnext);
		
		for (i=0; i<fbnb; i++) 
			sn->md->bt->insertValWrap(lba+k+i, fbpba+i);
		k += fbnb;
		fbe = fbnext;
	}
	if (k != nb) 
		cout << "Error: addLbaMap lba " << lba << " nb " << nb 
			<< " k " << k << endl;
}

// Whole file deduplication, for L type
void processWholeFile(struct FileBlocks *fb) {
	static unsigned int maxsize = 0;
	static unsigned char *buf = NULL;
	unsigned int i, n, size;
	HashList *hl;
	unsigned char filehash[16];
	unsigned int stent, nblks, refs;
	//unsigned int fbpba, fbnb, fbnext, fbent, fbentstart;
	unsigned long v;

	//clock_add(fb->fb_nb * 30);
	//t0_hash += (fb->fb_nb * 30);

	// Compute whole file hash, using hash of hashes(approximately)
	size = fb->fb_nb * 16;
	if (maxsize < size) {
		if (buf != NULL) free(buf);
		maxsize = size;
		buf = (unsigned char *) malloc(maxsize);
		assert(buf != NULL);
	}

	hl = fb->fb_hlist;
	for (i=0; i<fb->fb_nb; i++) {
		memcpy(buf+(i*16), hl->hl_hash, 16);
		hl = hl->hl_next;
	}
	md5(buf, size, filehash);

	n = filehash[0] % SNCOUNT;


	v = clock_get();
	// Search for the whole file hash value	
	snodes[n]->md->fhbt->searchValWrap(filehash, &stent, &nblks, &refs);
	if (stent == 0) {
		addNewFile(n, filehash, fb->fb_lbastart, fb->fb_nb);
		t0_dcomm += (((fb->fb_nb * 4096 + 1499) / 1500 + 1) * PKTCOMMTIME);
		snodes[n]->SEGCOUNT3 += 1;
		snodes[n]->SEGLENGTH3 += fb->fb_nb;
		snodes[n]->ublocks += fb->fb_nb;
	}
	else {
		// Check if exact match is there?
		if (nblks == fb->fb_nb) {
			snodes[n]->md->fhbt->incrementRefCountWrap(filehash);
			addLbaMap(n, fb->fb_lbastart, fb->fb_nb, stent);
		}
		else {
			// Otherwise it cannot be handled now here????
			cout << "Error: Two files having same hash value, \n"
				<< "but different data, this cannot be handled now!" << endl;
			cout << "old : (" << stent << ", " << nblks 
				<< ", refs " << ")\n" << "new : (" 
				<< fb->fb_lbastart << ", " << fb->fb_nb 
				<< ", .. )" << endl;
		}
	}
	t0_mdcomm += PKTCOMMTIME;
	snodes[n]->t_cumulative += (clock_get() - v);
}


void displayStatistics(void) {
	unsigned long clientOnlyExecution;
	unsigned long snMaxExecution = 0;
	unsigned long allSNexecution = 0, t;
	double totalSpace = 0;
	int i;

	for (i=0; i<SNCOUNT; i++) {
		allSNexecution += snodes[i]->t_cumulative;
		t = (snodes[i]->t_cumulative/1000 + (snodes[i]->SEGCOUNT3 * 40));
		if (t > snMaxExecution) snMaxExecution = t;
	}

	clientOnlyExecution = t2_clk - t1_clk - allSNexecution;
	cout << "\n\nWriting:\n";
	cout << "===============================================\n";
	cout << "client only execution\t\t\t: " << clientOnlyExecution/1000 << " ms"
		<< "\nstorage node max execution\t\t: " << snMaxExecution << " ms"
		<< "\nCommunication time (data)\t\t: " << t0_dcomm / 1000 << " ms"
		<< "\nCommunication time (metadata/dedup)\t: " << t0_mdcomm/1000 << " ms"
		<< "\nPreprocessing time\t\t\t: " << t0_hash/1000 << " ms" << endl;
	cout << "Write throughput\t\t\t: " << (((DATA_BLOCK_WRITE_COUNT * 512.0 * 
		1000) / (clientOnlyExecution/1000.0 + snMaxExecution + (t0_hash + t0_dcomm + t0_mdcomm)/1000))/
		(1024*1024)) << " MB" << endl;

	allSNexecution = 0;
	snMaxExecution = 0;
	for (i=0; i<SNCOUNT; i++) {
		allSNexecution += snodes[i]->t_rcumulative;
		t = (snodes[i]->t_rcumulative/1000 + (snodes[i]->RSEGCOUNT3 * 40));
		if (t > snMaxExecution) snMaxExecution = t;
	}

	cout << "\n\nReading:\n";
	cout << "===============================================\n";
	clientOnlyExecution = t3_clk - t2_clk - allSNexecution;
	cout << "client only execution\t\t\t: " << clientOnlyExecution/1000 << " ms"
		<< "\nstorage node max execution\t\t: " << snMaxExecution << " ms"
		<< "\nCommunication time (all)\t\t: " << t0_rcomm/1000 << " ms" << endl;
	cout << "Read throughput\t\t\t\t: " << (((DATA_BLOCK_READ_COUNT * 512.0 * 
		1000) / (clientOnlyExecution/1000.0 + snMaxExecution + t0_rcomm/1000))/
		(1024*1024)) << " MB" << endl;
	cout << "\n\nDisk space:\n";
	cout << "===============================================\n";
	cout << "Data stored on each stoarge node:\n";
	for (i=0; i<SNCOUNT; i++) {
		cout << "Node#" << (i+1) << "\t\t\t: "
			<< snodes[i]->UNIQUE_BLOCK_COUNT*4 << " KB\n";
		totalSpace += snodes[i]->UNIQUE_BLOCK_COUNT * 4;
	}
	cout << "Total Data stored\t: " << totalSpace << " KB\n";
	cout << "Working set size\t: " << WSS << " KB\n";
	cout << "Space saved\t\t: " << ((WSS - totalSpace)*100.0 / WSS) << "%\n";
}

int main(int argc, char** argv) {
	int i; 
	//int op; 
	//unsigned int pbasize = 0;
	//unsigned int *pba = NULL;
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	//unsigned int twritecount;
	int retval; 
	//unsigned int ind;
	//unsigned long dtime, overhead;
	int tno = 0;
	struct FileBlocks fb;
	//int count;
	//unsigned long long int metadatasize;
	//struct timespec tval;
	char sndirs[64][6] = { 
		"sn1",  "sn2",  "sn3",  "sn4",  "sn5",  "sn6",  "sn7",  "sn8",
	       	"sn9",  "sn10", "sn11", "sn12", "sn13", "sn14", "sn15", "sn16",
	       	"sn17", "sn18", "sn19", "sn20", "sn21", "sn22", "sn23", "sn24",
	       	"sn25", "sn26", "sn27", "sn28", "sn29", "sn30", "sn31", "sn32",
	       	"sn33", "sn34", "sn35", "sn36", "sn37", "sn38", "sn39", "sn40",
		"sn41", "sn42", "sn43", "sn44", "sn45", "sn46", "sn47", "sn48",
	       	"sn49", "sn50", "sn51", "sn52", "sn53", "sn54", "sn55", "sn56",
	       	"sn57", "sn58", "sn59", "sn60", "sn61", "sn62", "sn63", "sn64"};

#ifdef	DARCP
	unsigned long int DARCINTREFCOUNT;
	unsigned int darcInterval = 0;

	DARCINTREFCOUNT = 512 * (DATA_CACHE_SIZE / 512);
	unsigned long long int lastDecayTime = 0;
#endif
	StorageNode *client;

#ifdef	MAIL
	tno = 0;
#endif
#ifdef	WEB
	tno = 1;
#endif
#ifdef	HOME
	tno = 2;
#endif
#ifdef	LINUX
	tno = 3;
#endif
#ifdef	BOOKPPT
	tno = 4;
#endif
#ifdef	VIDIMG
	tno = 5;
#endif

	// Create the storage nodes
	for (i=0; i<SNCOUNT; i++) {
		snodes[i] = new StorageNode(sndirs[i], DEDUPAPP, DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
	}
	client = new StorageNode(".", DEDUPNATIVE, DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
	//cout << " main client 1" << endl;
	cfbl = new FBList("fblist.dat", FBLIST_CACHE_SIZE);
	frcp = new IIIBTree("filercpidx.dat", 64*1024, FILERECIPE_CACHE_LIST, 4096, client);
	frcp->enableCache(FILERECIPE_CACHE_SIZE);
	frcp->enableKeyValCache(FRECIPE_CACHE_SIZE);

	//cout << " main client 2" << endl;
	for (i=0; i<6; i++) {
		trace[i] = new IOTraceReader4K();

		if ((trace[i]->openTrace(fnames[i], fcount[i], 
				fbsize[i]) != 0)) {
			perror("IO Trace opening failure:");
			exit(0);
		}
	}

	//cout << " main client 3" << endl;
	//clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	//clock_gettime(CLOCK_REALTIME, &tval);
	//t1_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);

	t1_clk = clock_get();
	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	fb.fb_hlist = NULL;
	//unsigned int x;
	while (1) {
		retval = trace[tno]->readFile(&fb);

		if (retval == 0) break;	// End of the trace is reached

		//cout << "file " << x << " : " << fb.fb_nb << " == " << trace[tno]->getCurCount() << endl;
		//x++; continue;
		ts1 = fb.fb_ts1;
		ts2 = fb.fb_ts2;
		//op = fb.fb_rw;

		WRITE_REQ_COUNT++;
		OPERATION = OPERATION_WRITE;
		DATA_BLOCK_WRITE_COUNT += fb.fb_nb;

		fb.fb_nb /= 8;
		fb.fb_lbastart /= 8;

		// Create a file recipe with one entry
		// and remember that entry (last entry)

		if (fb.fb_nb > 1)
			incr = (ts2 - ts1) / (fb.fb_nb - 1);
		else	incr = 2;

		curTime = ts1;
		if (incr <= 0) incr = 2;

		t0_hash += (fb.fb_nb * 30);
		t0_mdcomm += (2 * PKTCOMMTIME);
		//cout << " main client 4" << endl;

		if (ftype[tno] == LTYPE) { // L type file
			processWholeFile(&fb);
		}
		else {	// Otherwise H or U type
			processHUFile(&fb);
		}
/*

	if (H or U type) {
		// Divide that into 1 MB super chunks
		for each super chunk {
			divide super chunk into 64KB segments
			compute min-hashes for each segment
			Compute multi-set bitmap vector
			Send min-hashes and bitmap vectors to group of data servers
			Find the number of distinct/unique blocks
			compute z = cj*wj (where wj = used storage capacity of j / average storage capacity used, cj is number of distict blocks to be stored)
			another equation z = duplicates / wj (wj minimum 1.0)
			Node j with Minimum z is selected
			send the super chunk to j
			Update file recipe locally
		}
	}
	else {
		compute finger prints for each block
		select k minimum fps
		Send these to group of data servers
		Compute the above parameter z.
		Select the data server j with minimum z.
		Send the whole file to j.
		Update file recipe locally
	}
*/













/*
		if (pbasize < fb.fb_nb) {
			if (pba != NULL) free(pba);
			pbasize = ((fb.fb_nb + 255)/256) * 256;
			pba = (unsigned int *)malloc(pbasize * sizeof(unsigned int));
			assert(pba != NULL);
		}

		clock_start();
		disk_start();

		if (fb.fb_nb <= SMALL_FILE_BLKS) {
			// Small file
			processSmallFile(&fb, md, pba);
		}
		else {
			// Large file
			if (ftype[tno] == LTYPE)
				processWholeFile(&fb, md, pba);
			else	processHULargeFile(&fb, md, pba);
		}
		writeFileData(pba, fb.fb_nb, ts1, incr);

		overhead = clock_stop();
		dtime = disk_stop();

		ind = dtime * 1000 + overhead;
#ifdef  HOME
		update_statistics(w_var, w_mean, w_min, w_max, w_n, fb.fb_nb, ind);
#else
		update_statistics(w_var, w_mean, w_min, w_max, w_n, fb.fb_nb*8, ind);
#endif

		//cout << "Step 1" << endl;
		countSegmentLength(pba, fb.fb_nb);
*/
		if (trace[tno]->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace[tno]->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

		//if ((lastGCTime == 0) && (lastFlushTime == 0)) { 
			// Not yet initialized
			//lastFlushTime = lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			clock_start();
			disk_start();

			md->flushCache();
			updateBitMap();
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);

			overhead = clock_stop();
			dtime = disk_stop();

			ind = dtime * 1000 + overhead; // Microseconds
			w_flush += ind;			
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			md->flushCache();
			updateBitMap();
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef  DARCP
                if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
                        //dc.decayWeights();
                        //ldc.decayWeights();
                        lastDecayTime = ts2 / 1000000;
                }


                // New interval starting, once per every 10 minutes
		if (((darcInterval + 1) * DARCINTREFCOUNT) > 
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
                        dc.newInterval();
			darcInterval++;
                }
#endif

		// Garbage collection once in an hour
		//if (((ts2 / 1000000) - lastGCTime) > (60000 * 60)) {
			//gc();
			//lastGCTime = ts2 / 1000000;
		//}
		//cout << "Step 2" << endl;
	}

	//clock_start();
	//disk_start();

	for (i=0; i<SNCOUNT; i++) {
		snodes[i]->md->flushCache();
		snodes[i]->bmap->updateBitMap();
	}

	//twritecount = dc.flushCache(dummy, 0, DATADISK);
	//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
	//DATA_BLOCK_FLUSH_WRITES += twritecount;

        //overhead = clock_stop();
        //dtime = disk_stop();

        //ind = dtime * 1000 + overhead; // Microseconds
        //w_flush += ind;

        cout << "\n\nTotal write records processed \t\t\t: " << trace[tno]->getCurCount() << endl;

	//clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	//clock_gettime(CLOCK_REALTIME, &tval);
	//t2_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	
	//t2_disk0 = DISK_WRITE_TOT_TIME[0];
	//t2_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];
	cfbl->flushCache();
	frcp->flushCache(METADATADISK);
	//frcp->display(&(frcp->bt_root), 1, 1);
	//exit(0);
	t2_clk = clock_get();
	//======================================
	cout << "\n============ READ Trace ("
		<< frnames[tno] << ") processing =========\n\n";	
	processReadTrace(tno);

	//=======================================

	//clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	//t3_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	//t3_disk0 = DISK_READ_TOT_TIME[0];
	//t3_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];
	t3_clk = clock_get();

	cout.setf(ios::fixed);
	cout.precision(4);
	displayStatistics();
/*
	cout << "\nt1_clk " << t1_clk << "\nt2_clk " << t2_clk << "\nt3_clk " << t3_clk << "\nt2_disk0 " << t2_disk0 << "\nt3_disk0 " << t3_disk0 << "\nt2_disk1 " << t2_disk1 << "\nt3_disk1 " << t3_disk1 << "\nt0_hash " << t0_hash << endl;

	cout << "Write throughput : " << (((DATA_BLOCK_WRITE_COUNT * 512.0 *
		10000000.0) / (t2_clk - t1_clk + t0_hash + ((t2_disk0 + t2_disk1)*1000)))
		/ (1024 * 1024)) << "MB / sec\n";
	cout << "Read throughput : " << (((DATA_BLOCK_READ_COUNT * 512.0 *
		10000000.0) / (t3_clk - t2_clk +
		((t3_disk0 + t3_disk1 - t2_disk1)*1000)))
		/ (1024 * 1024)) << "MB / sec\n";
*/
/*
	cout << "Working set size : " << WSS << "KB" << endl;
	cout << "Cache sizes used:\n" << "Data Cache size \t: " 
		<< (DATA_CACHE_SIZE / (1024)) 
		<< " KB\n"; // << "Meta data cache size \t: " 
; 		<< ((BUCKET_CACHE_SIZE + HBUCKET_CACHE_SIZE +
; 			BUCKETINDEX_CACHE_SIZE + (2 * BLOCKINDEX_CACHE_SIZE3) + 
; 			LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE + 
; 			RID_CACHE_SIZE + WRB_CACHE_SIZE +
; 			HLPL_CACHE_SIZE + LPL_CACHE_SIZE) / (1024)) << " KB\n";

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


	//cout << "\n\nTotal records processed \t\t\t: " << trace[tno]->getCurCount() << endl;
	cout << "\n\nTotal Blocks (lba btree)\t\t\t: " << md->bt->keycount << endl
		<< "Unique blocks identified (pb btree)\t: " << md->pb->keycount << endl
		<< "Unique blocks identified (Allocated) \t: " << UNIQUE_BLOCK_COUNT << endl;

	md->displayStatistics();

	display_io_response();
	cout << "\n\nData cache statistics \n";
	dc.displayStatistics();

	cout << "SEGCOUNT3 : " << SEGCOUNT3 << " SEGLENGTH3 : " << SEGLENGTH3 << " AVG SEGMENT LENGTH : " << (SEGLENGTH3 / SEGCOUNT3) << endl;
*/
	trace[tno]->closeTrace();	

	return 0;
}
