#include <iostream>
#include <fstream>
#include "dedupconfig.h"
#include "dedupn.h"
#include "bitmap.h"
#include "buckcomn.h"
#include "ibtree.h"
#include "hbtree.h"
#include "fhbtree.h"
#include "filerecipe.h"
#include "bucket.h"
#include "hbucket.h"
#include "cache.h"
#include "cachesize.h"
#include "cachemem.h"
#include "pbaref.h"
#include "writebuf.h"


//struct fpCacheEntry     *fp_head[FP_CACHE_LIST];
//struct fpCacheEntry     *fp_tail[FP_CACHE_LIST];
//struct fpCacheEntry     *fp_thead = NULL, *fp_ttail = NULL;
unsigned long           fp_size = 0;
unsigned int            fp_count = 0;

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
unsigned long long int DATA_BLOCK_FLUSH_WRITES = 0;

unsigned long long int UNIQUE_BLOCK_COUNT = 0;

unsigned long long int DISK_READ_COUNT[2] = {0, 0};
unsigned long long int DISK_READ_BCOUNT[2] = {0, 0};
unsigned long long int DISK_WRITE_COUNT[2] = {0, 0};
unsigned long long int DISK_WRITE_BCOUNT[2] = {0, 0};

double DISK_READ_TOT_TIME[2] = {0, 0};          // Time in mille seconds
double DISK_WRITE_TOT_TIME[2] = {0, 0};
double DISK_READ_MIN_TIME[2] = {10000, 10000};  // Time in mille seconds
double DISK_READ_MAX_TIME[2] = {0, 0};
double DISK_WRITE_MIN_TIME[2] = {10000, 10000}; // Time in mille seconds
double DISK_WRITE_MAX_TIME[2] = {0, 0};
unsigned long int DISK_READ_REQ_COUNT[1028][1000];
unsigned long int DISK_WRITE_REQ_COUNT[1028][1000];
unsigned long int DISK_IWRITE_REQ_COUNT[1028][1000];
unsigned long long int READ_REQ_COUNT = 0;

int main() {
	//long sizeval;
	cout << "cache entry " << sizeof(CacheEntry) << " long long " << sizeof(unsigned long long int) << " long " << sizeof(unsigned long) << " int " << sizeof(unsigned int) << endl;
	cout << "size of DISK_READ_REQ_COUNT array " << sizeof(DISK_READ_REQ_COUNT) << endl;
	cout << "WSS : " << WSS << endl;
	//cout << "DATA CACHE SIZE NATIVE : " << DATA_CACHE_SIZE_NATIVE << endl;
	cout << "DATA CACHE SIZE NATIVE(KB) : " << DATA_CACHE_SIZE_NATIVE/1024 << endl;

//	cout << "Bucket dedup Cache sizes used:\n" << "Data Cache size \t: "
//		<< (DATA_CACHE_SIZE / (1024))
//		<< " KB\n" << "Meta data cache size \t: "
//		<< ((BUCKET_CACHE_SIZE + HBUCKET_CACHE_SIZE +
//			BUCKETINDEX_CACHE_SIZE + BLOCKIINDEX_CACHE_SIZE +
//			LBA2PBA_CACHE_SIZE + LBA2BUCK_CACHE_SIZE +
//			PBAREF_CACHE_SIZE) / (1024)) << " KB\n";

	cout << "Bucket cache size \t\t:" << BUCKET_CACHE_SIZE/1024 << endl
	       << "HBucket cache size \t\t:" << HBUCKET_CACHE_SIZE/1024 << endl
	       << "Bktindex cache size \t\t:" << BUCKETINDEX_CACHE_SIZE/1024 << endl
	       << "Block index cache size\t\t:" << BLOCKINDEX_CACHE_SIZE/1024 << endl
	       << "Block3 index cache size\t\t:" << BLOCKINDEX_CACHE_SIZE3/1024 << endl
	       << "LBA2PBA cache size \t\t:" << LBA2PBA_CACHE_SIZE/1024 << endl
	       << "PBA2BUCK cache size \t\t:" << PBA2BUCK_CACHE_SIZE/1024 << endl
	       << "RID cache size \t\t:" << RID_CACHE_SIZE/1024 << endl
	       << "Wrb cache size \t\t:" << WRB_CACHE_SIZE/1024 << endl
	       << "lpl cache size \t\t:" << LPL_CACHE_SIZE/1024 << endl
	       << "hlpl cache size \t\t:" << HLPL_CACHE_SIZE/1024 << endl
	       << "fp cache size \t\t:" << FP_CACHE_SIZE/1024 << endl
	       << "Hash index cache size \t\t" << HASHINDEX_CACHE_SIZE/1024 << endl
	      << "PbaRef cache size \t\t:" << PBAREF_CACHE_SIZE/1024  << endl
	      << endl << endl;

	cout << "Bucket dedup #2 Cache sizes used:\n" << "Data Cache size \t: "
		<< (DATA_CACHE_SIZE / (1024))
		<< " KB\n" << "Meta data cache size \t: "
		<< (BUCKET_CACHE_SIZE + (long)HBUCKET_CACHE_SIZE +
			BUCKETINDEX_CACHE_SIZE + (2*BLOCKINDEX_CACHE_SIZE3) +
			LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE +
			RID_CACHE_SIZE + 
			LPL_CACHE_SIZE + HLPL_CACHE_SIZE) / (1024)
		<< " KB\n";

	cout << "\nAPP dedup cache sizes used:\n" 
		<< "Meta data cache size \t: "
		<< (2*BLOCKINDEX_CACHE_SIZE4 + HBUCKET_CACHE_SIZE +
                HLPL_CACHE_SIZE + FHBINDEX_CACHE_SIZE + FHB_CACHE_SIZE +
		LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE) / 1024 << "KB"
		<< "\n\t\t\t\t: "
		<< (2*BLOCKINDEX_CACHE_SIZE4 + HBUCKET_CACHE_SIZE +
                HLPL_CACHE_SIZE + BUCKET_CACHE_SIZE + BUCKETINDEX_CACHE_SIZE +
                LPL_CACHE_SIZE + RID_CACHE_SIZE +
		LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE) / 1024 << "KB\n";

	cout << "Full dedup Cache sizes used:\n" << "Data Cache size \t\t: "
		<< (DATA_CACHE_SIZE / (1024))
		<< " KB\n" << "Meta data cache size \t\t: "
		<< ((HASHINDEX_CACHE_SIZE + BLOCKINDEX_CACHE_SIZE +
			FP_CACHE_SIZE + PBAREF_CACHE_SIZE +
			LBA2PBA_CACHE_SIZE) / (1024)) << " KB\n";

//	cout << "\n\nCache sizes used:\n" << "Data Cache size : "
//		<< (DATA_CACHE_SIZE / (1024))
//		<< " KB\n" << "Meta data cache size : "
//		<< ((BUCKET_CACHE_SIZE + BUCKETINDEX_CACHE_SIZE +
//		(2 * BLOCKINDEX_CACHE_SIZE) + LBA2PBA_CACHE_SIZE +
//			PBA2BUCK_CACHE_SIZE) / (1024))
//		<< " KB\n";

//	cout << "\n\nCache sizes used:\n" << "Data Cache size : "
//		<< (DATA_CACHE_SIZE / (1024))
//		<< " KB\n" << "Meta data cache size : "
//		<< ((HASHINDEX_CACHE_SIZE + BLOCKINDEX_CACHE_SIZE +
//			FP_CACHE_SIZE + PBAREF_CACHE_SIZE) / (1024))
//		<< " KB\n";

	cout << "hash bucket size : " << sizeof(struct hashbucket) << endl;
	cout << "bucket size : " << sizeof(struct bucket) << endl;
	//cout << "bucketMem size : " << sizeof(struct bucketMem) << endl;
	//cout << "intintbtree node : " << sizeof(struct INTINTBTNode) << endl;
	//cout << "File recipe BTNode node : " << sizeof(struct IntIntShortBTNode) << endl;
	cout << "FileHashBTNode node : " << sizeof(struct FileHashBTNode) << endl;
	cout << "FileIntIntIntBTNode node : " << sizeof(struct IntIntIntBTNode) << endl;
	cout << "HashBTNode node : " << sizeof(struct HashBTNode) << endl;
	cout << "INTBTNode node : " << sizeof(struct INTBTNode) << endl;
	//cout << "fpCache entry : " << sizeof(fpCacheEntry) << endl;
	cout << "wrb entry : " << sizeof(WriteReqBuf) << endl;
	cout << "pbaref entry : " << sizeof(PbaRef) << endl;
	cout << "pbaList entry : " << sizeof(pbaList) << endl;
	//cout << "lbaBnoPba entry : " << sizeof(lbaBnoPba) << endl;

	//cout << "fpCache list : " << FP_CACHE_SIZE / sizeof(fpCacheEntry) << endl;
	cout << "wrb list : " << WRB_CACHE_SIZE / sizeof(WriteReqBuf) << endl;
	cout << "pbaref list : " << PBAREF_CACHE_SIZE / sizeof(PbaRef) << endl;
	//cout << "lbaBnoPba list : " << LPL_CACHE_SIZE / sizeof(lbaBnoPba) << endl;
	cout << "RID list : " << RID_CACHE_SIZE / sizeof(int) << endl;
	//cout << "CEMCOUNT : " << CEMCOUNT << endl;
	//cout << "cem count : " << ((DATA_CACHE_SIZE / (4*1024)) + ((BUCKET_CACHE_SIZE2 + HBUCKET_CACHE_SIZE2 + BUCKETINDEX_CACHE_SIZE + BLOCKINDEX_CACHE_SIZE3 * 2)/512) + (LBP_CACHE_SIZE + RID_CACHE_SIZE + WRBMCOUNT
	
	cout << "\n\nWSS " << WSS << endl << endl;
	cout << "DATA_CACHE_SIZE\t:" << DATA_CACHE_SIZE << endl
		<< "BUCKET_CACHE_SIZE\t:" << BUCKET_CACHE_SIZE << endl
		<< "HBUCKET_CACHE_SIZE\t:" << HBUCKET_CACHE_SIZE << endl
		<< "HASHINDEX_CACHE_SIZE\t:" << HASHINDEX_CACHE_SIZE << endl
		<< "FHBINDEX_CACHE_SIZE\t:" << FHBINDEX_CACHE_SIZE << endl
		<< "BLOCKINDEX_CACHE_SIZE\t:" << BLOCKINDEX_CACHE_SIZE << endl
		<< "BLOCKINDEX_CACHE_SIZE3\t:" << BLOCKINDEX_CACHE_SIZE3 << endl
		<< "BLOCKINDEX_CACHE_SIZE4\t:" << BLOCKINDEX_CACHE_SIZE4 << endl
		<< "BUCKETINDEX_CACHE_SIZE\t:" << BUCKETINDEX_CACHE_SIZE << endl
		<< "LBA2PBA_CACHE_SIZE\t:" << LBA2PBA_CACHE_SIZE << endl
		<< "PBA2BUCK_CACHE_SIZE\t:" << PBA2BUCK_CACHE_SIZE << endl
		<< "RID_CACHE_SIZE\t:" << RID_CACHE_SIZE << endl
		<< "PBAREF_CACHE_SIZE\t:" << PBAREF_CACHE_SIZE << endl
		<< "FP_CACHE_SIZE\t:" << FP_CACHE_SIZE << endl
		<< "FHB_CACHE_SIZE\t:" << FHB_CACHE_SIZE << endl
		<< "WRB_CACHE_SIZE\t:" << WRB_CACHE_SIZE << endl
		<< "LPL_CACHE_SIZE\t:" << LPL_CACHE_SIZE << endl
		<< "HLPL_CACHE_SIZE\t:" << HLPL_CACHE_SIZE << endl
		<< "DATA_CACHE_SIZE_NATIVE\t:" << DATA_CACHE_SIZE_NATIVE << endl
		<< "B512M_1\t:" << B512M_1 << endl
		<< "B512M_2\t:" << B512M_2 << endl
		<< "B512MCOUNT\t:" << B512MCOUNT << endl;


	return 0;
}
