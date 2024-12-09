#include <iostream>
#include <fstream>
#include <assert.h>
#include "dedupconfig.h"
#include "dedupn.h"
#include "iotracereader.h"
#include "hbtree.h"
#include "ibtree.h"
#include "cachesize.h"
#include "cachemem.h"

char fnames[][100] = {
/*
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.1.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.2.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.3.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.4.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.5.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.6.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.7.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.8.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.9.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.10.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.11.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.12.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.13.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.14.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.15.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.16.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.17.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.18.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.19.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.20.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.21.blkparse"
*/
/*
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.2.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.3.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.4.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.5.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.6.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.7.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.8.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.9.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.10.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.11.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.12.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.13.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.14.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.15.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.16.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.17.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.18.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.19.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.20.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.21.blkparse"
*/

"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.1.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.2.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.3.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.4.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.5.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.6.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.7.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.8.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.9.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.10.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.11.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.12.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.13.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.14.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.15.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.16.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.17.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.18.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.19.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.20.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.21.blkparse"
/*
*/

/*
"../../../../mail-01/sample-10000"

"../input/sample-10000"
*/

/*
	"../input/cheetah.cs.fiu.edu-110108-113008.2.blkparse"
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
*/
};

string blkidxfile = "blockidx.dat";
string hashidxfile = "hashidx.dat";

IBTree bt(blkidxfile.c_str(), 64*1024, 100000, 128);
HASHBTree hbt(hashidxfile.c_str(), 64*1024, 100000, 256);
INTBTNode b1;
HashBTNode b2;
LRU dc("datacache", DATA_CACHE_LIST, DATA_CACHE_SIZE);

string dummyfile="/dev/null";
fstream dummy;

unsigned int reccount100000 = 1;
unsigned long long int curTime;

/************************************************/
unsigned long long int DATA_BLOCK_READ_CACHE_HIT = 0;
unsigned long long int DATA_BLOCK_READ_COUNT = 0;
unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT = 0;
unsigned long long int DATA_BLOCK_WRITE_COUNT = 0;
unsigned long long int DATA_BLOCK_EFFECTIVE_WRITES = 0;

unsigned long long int UNIQUE_BLOCK_COUNT = 0;

unsigned long long int DISK_READ_COUNT[2] = {0, 0};
unsigned long long int DISK_READ_BCOUNT[2] = {0, 0};
unsigned long long int DISK_WRITE_COUNT[2] = {0, 0};
unsigned long long int DISK_WRITE_BCOUNT[2] = {0, 0};
unsigned long long int DISK_READ_SEG_LENGTH[2] = {0, 0};	// No of blocks
unsigned long long int DISK_WRITE_SEG_LENGTH[2] = {0, 0};
unsigned long long int DISK_READ_SEG_COUNTS[4][8*MAX_SEG_SIZE+8] = {{0}, {0}};
unsigned long long int DISK_WRITE_SEG_COUNTS[4][8*MAX_SEG_SIZE+8] = {{0}, {0}};

double DISK_READ_TOT_TIME[2] = {0, 0};		// Time in mille seconds
double DISK_WRITE_TOT_TIME[2] = {0, 0};
double DISK_READ_MIN_TIME[2] = {10000, 10000};	// Time in mille seconds
double DISK_READ_MAX_TIME[2] = {0, 0};
double DISK_WRITE_MIN_TIME[2] = {10000, 10000};	// Time in mille seconds
double DISK_WRITE_MAX_TIME[2] = {0, 0};

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

unsigned long long int FP_CACHE_HIT = 0;
unsigned long long int FP_CACHE_MISS = 0;
unsigned long long int LBA2PBA_CACHE_HIT = 0;
unsigned long long int LBA2PBA_CACHE_MISS = 0;
unsigned long long int PBAREF_CACHE_HIT = 0;
unsigned long long int PBAREF_CACHE_MISS = 0;
unsigned long long int READ_REQ_COUNT;
unsigned long long int WRITE_REQ_COUNT;
unsigned long int DISK_IWRITE_REQ_COUNT[1028][500];
unsigned long int DISK_READ_REQ_COUNT[1028][500];
unsigned long int DISK_WRITE_REQ_COUNT[1028][500];
/************************************************/

unsigned long int prevblock[2];	// Two disks



int OPERATION;

double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

///////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
		
	//IORecord rec;			// I/O record
	IOTraceReader *trace;
#ifdef	HOME
	struct segmenthome s;
#else
	struct segment4k s;
#endif

	int pos;
	unsigned int nodeno, lba;
	//struct Vertex *v1; //, *v2;
	int i, pid, op;
	unsigned long long int ts1, ts2;

	initCacheMem();
	cout << " Size of BTNode : " << sizeof(b1) << endl;
	bt.enableCache(BLOCKINDEX_CACHE_SIZE);
	hbt.enableCache(HASHINDEX_CACHE_SIZE);

	dummy.open(dummyfile.c_str(), ios::in | ios::out | ios::binary);
#ifdef	HOME
	trace = new IOTraceReaderHome();
	//if (trace.openTrace(fnames, 7) != 0) {
	if (trace->openTrace(fnames, 21, 512) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#else
	trace = new IOTraceReader4K();
	//if (trace.openTrace(fnames, 7) != 0) {
	if (trace->openTrace(fnames, 21, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#endif

	while (trace->readNextSegment(&pid, &op, &ts1, &ts2, &s)) {
#ifndef	HOME
		if (op == OP_READ) DATA_BLOCK_READ_COUNT += (s.seg_size*8);
		else DATA_BLOCK_WRITE_COUNT += (s.seg_size*8);
#else
		if (op == OP_READ) DATA_BLOCK_READ_COUNT += (s.seg_size);
		else DATA_BLOCK_WRITE_COUNT += (s.seg_size);
#endif

		for (i=0; i<s.seg_size; i++) {
			hbt.searchVal(s.seg_hash[i], &nodeno, &pos, &lba, &hbt.bt_root, 1);
			if (nodeno == 0) {
				hbt.insertVal(s.seg_hash[i], s.seg_start + i);
			}
		}

		for (i=0; i<s.seg_size; i++) {
#ifndef	HOME
			bt.searchVal(s.seg_start+(i*8), &nodeno, &pos, &lba, &bt.bt_root, 1);
			if (nodeno == 0) {
				bt.insertVal(s.seg_start+(i*8), s.seg_start + (i*8));
			}
#else
			bt.searchVal(s.seg_start+i, &nodeno, &pos, &lba, &bt.bt_root, 1);
			if (nodeno == 0) {
				bt.insertVal(s.seg_start+i, s.seg_start + i);
			}
#endif
		}

		if (trace->getCurCount() >= (reccount100000 * 500000)) {
			cout << trace->getCurCount() << "  number of records processed\n";
			reccount100000++;
		}

	}

	bt.flushCache(METADATADISK);
	hbt.flushCache(METADATADISK);
	cout << "Total read requests \t\t:" << DATA_BLOCK_READ_COUNT << endl
		<< "Total write requests \t\t:" << DATA_BLOCK_WRITE_COUNT << endl;
	cout << "\n\nBTree index 2\n";
	bt.displayStatistics();
	hbt.displayStatistics();

	trace->closeTrace();	

	return 0;
}
