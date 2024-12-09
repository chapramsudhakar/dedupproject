#include <iostream>
#include <fstream>
#include <assert.h>
//#include "dedupconfig.h"
#include "dedupn.h"
#include "hbtree.h"
#include "cachesize.h"

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

int main(int argc, char *argv[]) {
	
	HASHBTree hbt(argv[1], 4096, HASHINDEX_CACHE_LIST, 128);

	hbt.countKeys(&hbt.bt_root, 0);
	//hbt.enumKeys(&hbt.bt_root, 1, 1);
	return 0;
}

