#include<iostream>
#include<fstream>
#include<iomanip>
#include<cstring>
#include<assert.h>
#include "dedupn.h"
#include "cachesize.h"
#include "hbucket.h"
#include "ibtree.h"
#include "cachemem.h"

using namespace std;

HashBucket	*bkt;
unsigned long long int curTime = 0;

string dummyfile="/dev/null";
fstream dummy;
unsigned long int prevblock[2];

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
unsigned int DISK_READ_SEG_LENGTH = 0;	// No of blocks
unsigned int DISK_WRITE_SEG_LENGTH = 0;
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;
unsigned long int DISK_READ_REQ_COUNT[1028][500];
unsigned long int DISK_WRITE_REQ_COUNT[1028][500];
unsigned long int DISK_IWRITE_REQ_COUNT[1028][500];

unsigned int DISK_READ_SEG_COUNTS[2][MAX_SEG_SIZE] = {{0}};
unsigned int DISK_WRITE_SEG_COUNTS[2][MAX_SEG_SIZE] = {{0}};
double DISK_READ_TOT_TIME[2] = {0};		// Time in mille seconds
double DISK_WRITE_TOT_TIME[2] = {0};
double DISK_READ_MIN_TIME[2] = {10000};	// Time in mille seconds
double DISK_READ_MAX_TIME[2] = {0};
double DISK_WRITE_MIN_TIME[2] = {10000};	// Time in mille seconds
double DISK_WRITE_MAX_TIME[2] = {0};

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

/************************************************/
//ofstream dsim;
string pbaindex = "pbaidx.dat";

IBTree pb(pbaindex.c_str(), 4096, BLOCKINDEX_CACHE_LIST, 128);

int OPERATION;

double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

void bsort(unsigned int a[], unsigned int n) {
	unsigned int i, j;
	unsigned int t;

	for (i=0; i<n-1; i++) {
		for (j=0; j<n-i-1; j++) {
			if (a[j] > a[j+1]) {
				t = a[j];
				a[j] = a[j+1];
				a[j+1] = t;
			}
		}
	}
}

int main(int argc, char** argv) {
		
	struct hashbucket stb1;
	unsigned int ucount = 0;
	unsigned int blks[BLKS_PER_HBKT] = {0};
	unsigned int depth[101] = {0};
	unsigned int linkbucket = 0, tl, lt;
	unsigned int i; //, j, len;
	unsigned int chains = 0;

	initCacheMem();
	bkt = new HashBucket();

	for (i=1; i<=64*1024; i++) {
		bkt->readHBucket(i, &stb1);

		if (stb1.hb_nent != 0) {
			ucount++;
			blks[stb1.hb_nent-1]++;

			// Beginning of linked list of buckets
			// then find depth
			tl = 1;	
			lt = stb1.hb_nextbucket;
			while (lt > 0) {
				bkt->readHBucket(lt, &stb1);
				ucount++;
				blks[stb1.hb_nent-1]++;
				tl++;
				lt = stb1.hb_nextbucket;
			}
			if (tl <= 100) depth[tl-1]++;
			else 	depth[100]++;
			if (tl > linkbucket) linkbucket = tl;
		}
	}
	
	cout << "Max buckets : " << MAX_HBUCKETS << endl
		<< "used count : " << ucount << endl
		<< "Maximum link bucket depth : " << linkbucket << endl << endl;
	
	for (i=0; i<BLKS_PER_HBKT; i++)
		if (blks[i] != 0)
		cout << "Block count -- " << i+1 << " ==> " << blks[i] << endl;
	cout << "\n\nDepth of linked buckets\n";
	for (i=0; i<101; i++) 
		if (depth[i] != 0) {
			cout << "Depth -- " << i+1 << " ==> " << depth[i] << endl;
			chains += depth[i];
		}
	cout << "\n\nTotal chains : " << chains << endl;
		
	return 0;
}
