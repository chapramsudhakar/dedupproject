#include<iostream>
#include<fstream>
#include<iomanip>
#include<cstring>
#include<assert.h>
//#include "dedupconfig.h"
#include "dedupn.h"
#include "bucket.h"
#include "cachemem.h"
#include "metadata.h"

using namespace std;

Bucket	*bkt;

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

unsigned long int DISK_READ_REQ_COUNT[1028][500];
unsigned long int DISK_WRITE_REQ_COUNT[1028][500];
unsigned long int DISK_IWRITE_REQ_COUNT[1028][500];
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;


double DISK_READ_TOT_TIME = 0;		// Time in mille seconds
double DISK_WRITE_TOT_TIME = 0;
double DISK_READ_MIN_TIME = 10000;	// Time in mille seconds
double DISK_READ_MAX_TIME = 0;
double DISK_WRITE_MIN_TIME = 10000;	// Time in mille seconds
double DISK_WRITE_MAX_TIME = 0;

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

/************************************************/
string pbaindex = "pbaidx.dat";

IBTree pb(pbaindex.c_str(), 4096, 1024, 128);

int OPERATION;

double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

MDMaps *md;

void getPbaRefHash(unsigned int pba, unsigned char hash[]) {
	return;
}
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

void ridBktDeleteWrap1(unsigned int rid) {
}
void ridBktDeleteWrap(unsigned char *hash) {
}

unsigned int lbaBnoEmpty(unsigned int bno) {
        return 1;
}

unsigned int segsize[BLKS_PER_BKT] = {0};
unsigned int blks[BLKS_PER_BKT] = {0};
unsigned int depth[101] = {0};

int main(int argc, char** argv) {
		
	struct bucket stb1;
	unsigned int ucount = 0;
	unsigned int tsegcount = 0;
	unsigned int linkbucket = 0, tl, lt;
	unsigned int i, j, len;
	float segsum = 0.0f, segwcount = 0.0f;
	float avgdepth =0.0f;
	int bktcount = 0;

	initCacheMem();

	bkt = new Bucket(64*1024, 1024, 128);

	for (i=2; i<=MAX_BUCKETS; i++) {
		bkt->readBucket(i, &stb1);


		if (stb1.b_nent != 0) {
			ucount++;
			blks[stb1.b_nent-1]++;

			// Sort
#ifdef BUCKET3
			bsort(stb1.b_pba, stb1.b_nent);
#else
			bsort(stb1.b_lba, stb1.b_nent);
#endif

			tsegcount++;
			len = 1;

			for (j=1; j<stb1.b_nent; j++) {

#ifdef BUCKET3
				if ((stb1.b_pba[j-1]+1) == stb1.b_pba[j]) len++;
#else
				if ((stb1.b_lba[j-1]+1) == stb1.b_lba[j]) len++;
#endif
				else {
					// New segment start
					segsize[len-1]++;
					tsegcount++;
					len = 1;
				}
			}
			// Last segment
			segsize[len-1]++;
		
			if (stb1.b_prevbucket == 0) {
				// Beginning of linked list of buckets
				// then find depth
				tl = 1;	
				lt = stb1.b_nextbucket;
				while (lt > 0) {
					bkt->readBucket(lt, &stb1);
					tl++;
					lt = stb1.b_nextbucket;
				}
				if (tl <= 100)
					depth[tl-1]++;
				else 	depth[100]++;
				if (tl > linkbucket) linkbucket = tl;
			}
		}
	}
	
	cout << "Max buckets : " << MAX_BUCKETS << endl
		<< "used count : " << ucount << endl
		<< "Total number of segments : " << tsegcount << endl
		<< "Maximum link bucket depth : " << linkbucket << endl << endl;
	
	for (i=0; i<BLKS_PER_BKT; i++) {
		if (segsize[i] != 0) {
			cout << "Seg size -- " << i+1 << " ==> " << segsize[i] << endl;
			segsum += segsize[i];
			segwcount += (segsize[i] * (i+1));
		}
		
	}
	cout << "\n\nCount of buckets with block counts\n";
	for (i=0; i<BLKS_PER_BKT; i++)
		if (blks[i] != 0)
		cout << "Block count -- " << i+1 << " ==> " << blks[i] << endl;
	cout << "\n\nDepth of linked buckets\n";
	for (i=0; i<101; i++) 
		if (depth[i] != 0) {
			cout << "Depth -- " << i+1 << " ==> " << depth[i] << endl;
			avgdepth += ((i+1) * depth[i]);
			bktcount += depth[i];
		}

	cout.setf(ios::fixed);
	cout << "Segment length weighted : " << segwcount << endl;
	cout << "Segment count total : " << segsum << ", " << tsegcount << endl;
	cout << "Average segment length : " << (segwcount / segsum) << ", " << (segwcount / tsegcount) << endl;
	cout << "Average depth of a bucket chain : " << (avgdepth / bktcount) << endl;
		
	return 0;
}
