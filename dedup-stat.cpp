#include <iostream>
#include <fstream>
#include <assert.h>
#include "dedupconfig.h"
#include "dedupn.h"
#include "hbtree.h"
#include "ibtree.h"
#include "dupset.h"
#include "iotracereader.h"
#include "cachesize.h"


char fnames[][100] = {
/*
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.1.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.2.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.3.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.4.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.5.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.6.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.7.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.8.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.9.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.10.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.11.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.12.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.13.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.14.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.15.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.16.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.17.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.18.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.19.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.20.blkparse",
	"../../../../mail-01/cheetah.cs.fiu.edu-110108-113008.21.blkparse"
*/
/*
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.2.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.3.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.4.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.5.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.6.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.7.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.8.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.9.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.10.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.11.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.12.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.13.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.14.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.15.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.16.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.17.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.18.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.19.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.20.blkparse",
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.21.blkparse"
*/

/*
"../../../../mail-01/sample-10000"

"../input/sample-10000"
*/

/*
	"../input/cheetah.cs.fiu.edu-110108-113008.2.blkparse"
*/
"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
};

string blkidxfile = "blockidx.dat";
string hashidxfile = "hashidx.dat";

ofstream dsim("dsim.in", ios::out);

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

unsigned long int DISK_READ_REQ_COUNT[1028][500];
unsigned long int DISK_WRITE_REQ_COUNT[1028][500];
unsigned long int DISK_IWRITE_REQ_COUNT[1028][500];
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;

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

/************************************************/

unsigned long int prevblock[2];	// Two disks



int OPERATION;

double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

///////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
		
	IORecord4K rec;			// I/O record
	IOTraceReader trace;

	int pos;
	unsigned int nodeno, lba;
	struct Vertex *v1; 

	cout << " Size of BTNode : " << sizeof(b1) << endl;
	bt.enableCache(BLOCKINDEX_CACHE_SIZE);
	hbt.enableCache(HASHINDEX_CACHE_SIZE);

	dummy.open(dummyfile.c_str(), ios::in | ios::out | ios::binary);

	if (trace.openTrace(fnames, 1, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}

	while (trace.readNext(rec)) {
		if (rec.ior_rw == 'R') DATA_BLOCK_READ_COUNT++;
		else DATA_BLOCK_WRITE_COUNT++;

		// Search the duplicate set to find the node
		if ((v1 = findVertex(rec.ior_lba)) == NULL) {
			// If not found, check if this is a duplicate 
			// of another block. (By searching the hash index)
			hbt.searchVal(rec.ior_md5hash, &nodeno, &pos, &lba, &hbt.bt_root, 1);
			if (nodeno != 0) {
				// Duplicate node is found
				// Add the current node to the duplicate set
				if (rec.ior_lba != lba) // If not same
					v1 = addVertex(rec.ior_lba, lba);
				else cout << "Same node appeared\n";
			}
			else {
				// Add the current node to a new set
				v1 = addVertex(rec.ior_lba, rec.ior_lba);

				// If no duplicate found then service 
				// directly from the original block from the 
				// disk. Add this block details
				// to both lba index and hash index files 
				// and increment the 
				// unique block count.
				hbt.insertVal(rec.ior_md5hash, rec.ior_lba);
				bt.insertVal(rec.ior_lba, rec.ior_lba);
				UNIQUE_BLOCK_COUNT++;
			}
		}

		if (trace.getCurCount() >= (reccount100000 * 500000)) {
			cout << trace.getCurCount() << "  number of records processed\n";
			reccount100000++;
		}
	}

	cout << "Unique blocks \t\t:" << UNIQUE_BLOCK_COUNT << " / " << trace.getCurCount() << endl
		<< "Total read requests \t\t:" << DATA_BLOCK_READ_COUNT << endl
		<< "Total write requests \t\t:" << DATA_BLOCK_WRITE_COUNT << endl
		<< "Total read hits \t\t:" << DATA_BLOCK_READ_CACHE_HIT << endl
		<< "Total write hits \t\t:" << DATA_BLOCK_WRITE_CACHE_HIT << endl;
	cout << "\n\nBTree index 1\n";
	bt.displayStatistics();
	cout << "\n\nBTree index 2\n";
	hbt.displayStatistics();

	cout << "Data cache statistics \n";
	dc.displayStatistics();

	cout << "DISK_READ_COUNT : " << DISK_READ_COUNT << endl
		<< "DISK_WRITE_COUNT : " << DISK_WRITE_COUNT << endl
		<< "DISK_READ_SEG_LENGTH : " << DISK_READ_SEG_LENGTH << endl
		<< "DISK_WRITE_SEG_LENGTH : " << DISK_WRITE_SEG_LENGTH << endl
		<< "DISK_READ_TOT_TIME : " << DISK_READ_TOT_TIME << endl
		<< "DISK_WRITE_TOT_TIME : " << DISK_WRITE_TOT_TIME << endl
		<< "DISK_READ_MIN_TIME : " << DISK_READ_MIN_TIME << endl
		<< "DISK_READ_MAX_TIME : " << DISK_READ_MAX_TIME << endl
		<< "DISK_WRITE_MIN_TIME : " << DISK_WRITE_MIN_TIME << endl
		<< "DISK_WRITE_MAX_TIME : " << DISK_WRITE_MAX_TIME << endl;
	
	displayDupSet();

	trace.closeTrace();	

	return 0;
}
