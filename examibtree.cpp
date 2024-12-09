#include <iostream>
#include <fstream>
#include <assert.h>
//#include "dedupconfig.h"
#include"dedupn.h"
#include "ibtree.h"
#include "cachesize.h"
//#include "cachemem.h"


using namespace std;


unsigned long int prevblock[2];

unsigned long long int curTime;

/************************************************/
unsigned long long int DATA_BLOCK_READ_CACHE_HIT = 0;
unsigned long long int DATA_BLOCK_READ_COUNT = 0;
unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT = 0;
unsigned long long int DATA_BLOCK_WRITE_COUNT = 0;
unsigned long long int UNIQUE_BLOCK_COUNT = 0;

unsigned long long int DISK_READ_COUNT[2] = {0};
unsigned long long int DISK_READ_BCOUNT[2] = {0};
unsigned long long int DISK_WRITE_COUNT[2] = {0};
unsigned long long int DISK_WRITE_BCOUNT[2] = {0};
unsigned long long int DISK_READ_SEG_LENGTH[2] = {0};     // No of blocks
unsigned long long int DISK_WRITE_SEG_LENGTH[2] = {0};
unsigned long long int DISK_READ_SEG_COUNTS[4][8*MAX_SEG_SIZE+8] = {{0}};
unsigned long long int DISK_WRITE_SEG_COUNTS[4][8*MAX_SEG_SIZE+8] = {{0}};
double DISK_READ_TOT_TIME[2] = {0};             // Time in mille seconds
double DISK_WRITE_TOT_TIME[2] = {0};
double DISK_READ_MIN_TIME[2] = {10000}; // Time in mille seconds
double DISK_READ_MAX_TIME[2] = {0};
double DISK_WRITE_MIN_TIME[2] = {10000};        // Time in mille seconds
double DISK_WRITE_MAX_TIME[2] = {0};
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
unsigned long long int PBA2BUCK_CACHE_HIT = 0;
unsigned long long int PBA2BUCK_CACHE_MISS = 0;

/************************************************/
int OPERATION;
double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

unsigned int segs[2049];
unsigned int seglen[2049];
int main(int argc, char *argv[]) {
	IBTree bt(argv[1], 4096, BLOCKINDEX_CACHE_LIST, 128);
	unsigned int *key, *val;
	unsigned int count, count1;
	unsigned int segsize;
	unsigned int i, j, t;
	float segwcount = 0.0f, segsum = 0.0f;
	int diff;

	for (i=0; i<2019; i++) segs[i] = 0;

	count = bt.countKeys(&bt.bt_root, 0);

	key = (unsigned int *) malloc(sizeof(unsigned int) * (count + 100));
	val = (unsigned int *) malloc(sizeof(unsigned int) * (count + 100));

	if ((key == NULL) || (val == NULL)) {
		cout <<"Error malloc failue\n";
		exit(0);
	}

	count1 = bt.enumKeysVals(&bt.bt_root, 0, key, val, 0);
	i = 0;
	while (i < count1) {
		segsize = 1;
		i++;
		diff = val[i] - val[i-1];
#ifndef	BUCKET3
		while ((i < count1) && (segsize < 16) && ((key[i-1] + 1) == key[i]) &&
				(abs(diff) <= 1)) {
#else
		while ((i < count1) && (segsize < 16) && ((key[i-1] + 1) == key[i]) &&
				(abs(diff) <= 16)) {
#endif
			segsize++;
			i++;
			diff = val[i] - val[i-1];
		}
		if (segsize <= 2048)
			segs[segsize-1]++;
		else segs[2048]++;
	}

	for (i=0; i<2049; i++) seglen[i] = i+1;

	for (i=0; i<2049; i++) {
		segwcount += ((i+1) * segs[i]);
		segsum += segs[i];
	}

	for (i=0; i<2049; i++) {
		for (j=0; j<(2049-i-1); j++) {
			if (segs[j] < segs[j+1]) {
				t = segs[j];
				segs[j] = segs[j+1];
				segs[j+1] = t;
				t = seglen[j];
				seglen[j] = seglen[j+1];
				seglen[j+1] = t;
			}
		}
	}

	cout << "SegLen\tCount\n";
	for (i=0; i<=2048; i++) {
		if (segs[i] != 0) {
			cout << i+1 << "\t" << segs[i] << endl;
		}
	}
	cout.setf(ios::fixed);
	cout << "segwcount " << segwcount << " segsum " << segsum 
		<< " Average seg length : " << (segwcount / segsum) << endl;
	return 0;
}

