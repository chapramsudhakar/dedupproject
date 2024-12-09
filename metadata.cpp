#include <iostream>
#include "dedupconfig.h"
#include "dedupn.h"
#include "metadata.h"

using namespace std;

extern unsigned long long int DATA_BLOCK_READ_COUNT;
extern unsigned long long int DATA_BLOCK_WRITE_COUNT;

MDMaps::MDMaps(const char *lbamapname, const char *pbamapname,
	const char *hbtmapname, const char *fhbtmapname,
	const char *bktname, const char *bktidxname, const char *hbktname,
	const char *pbarefname, const char *fblname, int deduptype, StorageNode *sn)  {

	if (deduptype == DEDUPFULL) {
		//cout << "metadata: full" << endl;
		bt = new IBTree(lbamapname, 64*1024, BLOCKINDEX_CACHE_LIST, 4096, sn);
		bt->enableCache(BLOCKINDEX_CACHE_SIZE);
		bt->enableKeyValCache(LBA2PBA_CACHE_SIZE);

		hbt = new HASHBTree(hbtmapname, 64*1024, HASHINDEX_CACHE_LIST, 8192, sn);
		hbt->enableCache(HASHINDEX_CACHE_SIZE);
		hbt->enableKeyValCache(FP_CACHE_SIZE);

		pbaref = new PbaRefOps(pbarefname, PBAREF_CACHE_SIZE, sn);
		bkt = NULL;
		hbkt = NULL;
		fhbt = NULL;
		fbl = NULL;
		metaDataSize = BLOCKINDEX_CACHE_SIZE + HASHINDEX_CACHE_SIZE + 
			LBA2PBA_CACHE_SIZE + FP_CACHE_SIZE + PBAREF_CACHE_SIZE;
	}
	else if (deduptype == DEDUPAPP) {
		//cout << "metadata: app" << endl;
		bt = new IBTree(lbamapname, 64*1024, BLOCKINDEX_CACHE_LIST, 4096, sn);
		//cout << "BLOCKIDX CACHE " << BLOCKINDEX_CACHE_SIZE4 << endl;
		bt->enableCache(BLOCKINDEX_CACHE_SIZE4);
		bt->enableKeyValCache(LBA2PBA_CACHE_SIZE);

		pb = new IBTree(pbamapname, 64*1024, BLOCKINDEX_CACHE_LIST, 4096, sn);
		pb->enableCache(BLOCKINDEX_CACHE_SIZE4);
		pb->enableKeyValCache(PBA2BUCK_CACHE_SIZE);

		fhbt = new FileHASHBTree(fhbtmapname, 64*1024, BLOCKINDEX_CACHE_LIST, 4096, sn);
		fhbt->enableCache(FHBINDEX_CACHE_SIZE);
		fhbt->enableKeyValCache(FHB_CACHE_SIZE);

		fbl = new FBList(fblname, FBLIST_CACHE_SIZE);

		bkt = new Bucket(bktname, bktidxname, 64*1024, BUCKETINDEX_CACHE_LIST, 4096, sn);
		bkt->enableKeyValueCache(LPL_CACHE_SIZE, 64*1024);

		hbkt = new HashBucket(hbktname, sn);
		hbkt->enableKeyValueCache(HLPL_CACHE_SIZE, 1024);

		hbt = NULL;
		pbaref = NULL;
		metaDataSize = 2*BLOCKINDEX_CACHE_SIZE4 + HBUCKET_CACHE_SIZE + 
			HLPL_CACHE_SIZE + LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE;
#ifdef	VIDIMG
		metaDataSize += (FHBINDEX_CACHE_SIZE + FHB_CACHE_SIZE + FBLIST_CACHE_SIZE);
#else
		metaDataSize += (BUCKET_CACHE_SIZE + BUCKETINDEX_CACHE_SIZE + 
			LPL_CACHE_SIZE + RID_CACHE_SIZE);
#endif
	}
	else if (deduptype == DEDUPHDS) {
		//cout << "metadata: HDS" << endl;
		bt = new IBTree(lbamapname, 64*1024, BLOCKINDEX_CACHE_LIST, 4096, sn);
		bt->enableCache(BLOCKINDEX_CACHE_SIZE3);
		bt->enableKeyValCache(LBA2PBA_CACHE_SIZE);

		pb = new IBTree(pbamapname, 64*1024, BLOCKINDEX_CACHE_LIST, 4096, sn);
		pb->enableCache(BLOCKINDEX_CACHE_SIZE3);
		pb->enableKeyValCache(PBA2BUCK_CACHE_SIZE);

		bkt = new Bucket(bktname, bktidxname, 64*1024, BUCKETINDEX_CACHE_LIST, 4096, sn);
		bkt->enableKeyValueCache(LPL_CACHE_SIZE, 64*1024);

		hbkt = new HashBucket(hbktname, sn);
		hbkt->enableKeyValueCache(HLPL_CACHE_SIZE, 1024);

		hbt = NULL;
		fhbt = NULL;
		fbl = NULL;
		pbaref = NULL;

		metaDataSize = 2*BLOCKINDEX_CACHE_SIZE3 + HBUCKET_CACHE_SIZE + 
			BUCKET_CACHE_SIZE + BUCKETINDEX_CACHE_SIZE + 
			LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE + 
			LPL_CACHE_SIZE + RID_CACHE_SIZE + HLPL_CACHE_SIZE;
	}
	else {
		//cout << "metadata: native" << endl;
		bt = NULL; pb = NULL; bkt = NULL; hbkt = NULL;
		hbt = NULL; fhbt = NULL; pbaref = NULL; fbl = NULL;
		metaDataSize = 0;
	}
}

void MDMaps::flushCache(void) {
	bt->flushCache(METADATADISK);
	if (pb != NULL) pb->flushCache(METADATADISK);
	if (bkt != NULL) bkt->flushCache();
	if (hbkt != NULL) hbkt->flushCache();
	if (hbt != NULL) hbt->flushCache(METADATADISK);
	if (fhbt != NULL) fhbt->flushCache(METADATADISK);
	if (pbaref != NULL) pbaref->flushCache();
	if (fbl != NULL) fbl->flushCache();
}

void MDMaps::displayStatistics(void) {
	unsigned long long int insertops = 0, deleteops = 0;
	unsigned long long int updateops = 0, searchops = 0;
	double readOpReads = 0, readOpWrites = 0;
	double writeOpReads = 0, writeOpWrites = 0;

	cout << "\nMetadata Size : " << metaDataSize/1024 << "KB" << endl;
	cout << "\n========= BT ========\n";
	bt->displayStatistics();
	readOpReads += bt->BT_BTNODE_READCOUNT_READ;
	writeOpReads += bt->BT_BTNODE_READCOUNT_WRITE;
	readOpWrites += bt->BT_BTNODE_WRITECOUNT_READ;
	writeOpWrites += bt->BT_BTNODE_WRITECOUNT_WRITE;
	insertops += bt->BTINSERT;
	deleteops += bt->BTDELETE;
	updateops += bt->BTUPDATE;
	searchops += bt->BTSEARCH;

	if (pb != NULL) {
		cout << "\n========= PB ========\n";
		pb->displayStatistics();
		readOpReads += pb->BT_BTNODE_READCOUNT_READ;
		writeOpReads += pb->BT_BTNODE_READCOUNT_WRITE;
		readOpWrites += pb->BT_BTNODE_WRITECOUNT_READ;
		writeOpWrites += pb->BT_BTNODE_WRITECOUNT_WRITE;
		insertops += pb->BTINSERT;
		deleteops += pb->BTDELETE;
		updateops += pb->BTUPDATE;
		searchops += pb->BTSEARCH;
	}

	if (bkt != NULL) {
		cout << "\n========= BKT ========\n";
		bkt->displayStatistics();
		readOpReads += bkt->BUCKET_READ_COUNT_READ;
		writeOpReads += bkt->BUCKET_READ_COUNT_WRITE;
		readOpWrites += bkt->BUCKET_WRITE_COUNT_READ;
		writeOpWrites += bkt->BUCKET_WRITE_COUNT_WRITE;
		insertops += bkt->BKTINSERT;
		deleteops += bkt->BKTDELETE;
		updateops += bkt->BKTUPDATE;
		searchops += bkt->BKTSEARCH;
	}
	if (hbkt != NULL) {
		cout << "\n========= HBKT ========\n";
		hbkt->displayStatistics();
		readOpReads += hbkt->HBUCKET_READ_COUNT_READ;
		writeOpReads += hbkt->HBUCKET_READ_COUNT_WRITE;
		readOpWrites += hbkt->HBUCKET_WRITE_COUNT_READ;
		writeOpWrites += hbkt->HBUCKET_WRITE_COUNT_WRITE;
		insertops += hbkt->BKTINSERT;
		deleteops += hbkt->BKTDELETE;
		updateops += hbkt->BKTUPDATE;
		searchops += hbkt->BKTSEARCH;
	}
	if (hbt != NULL) {
		cout << "\n========= HBT ========\n";
		hbt->displayStatistics();
		readOpReads += hbt->BT_BTNODE_READCOUNT_READ;
		writeOpReads += hbt->BT_BTNODE_READCOUNT_WRITE;
		readOpWrites += hbt->BT_BTNODE_WRITECOUNT_READ;
		writeOpWrites += hbt->BT_BTNODE_WRITECOUNT_WRITE;
		insertops += hbt->BTINSERT;
		deleteops += hbt->BTDELETE;
		updateops += hbt->BTUPDATE;
		searchops += hbt->BTSEARCH;
	}
	if (fhbt != NULL) {
		cout << "\n========= FHBT ========\n";
		fhbt->displayStatistics();
		readOpReads += fhbt->BT_BTNODE_READCOUNT_READ;
		writeOpReads += fhbt->BT_BTNODE_READCOUNT_WRITE;
		readOpWrites += fhbt->BT_BTNODE_WRITECOUNT_READ;
		writeOpWrites += fhbt->BT_BTNODE_WRITECOUNT_WRITE;
		insertops += fhbt->BTINSERT;
		deleteops += fhbt->BTDELETE;
		updateops += fhbt->BTUPDATE;
		searchops += fhbt->BTSEARCH;
	}
	if (pbaref != NULL) {
		//pbaref->displayStatistics();
		//readOpReads += pbaref->PBREF_READCOUNT_READ;
		//writeOpReads += pbaref->PBREF_READCOUNT_WRITE;
		//readOpWrites += pbaref->PBREF_WRITECOUNT_READ;
		//writeOpWrites += pbaref->PBREF_WRITECOUNT_WRITE;
		//insertops += pbaref->PBREFINSERT;
		//deleteops += pbaref->PBREFDELETE;
		//updateops += pbaref->PBREFUPDATE;
		//searchops += pbaref->PBREFSEARCH;
	}
	if (fbl != NULL) {
		//fbl->displayStatistics();
		//insertops += fbl->FBEINSERT;
		//deleteops += fbl->FBEDELETE;
		//updateops += fbl->FBEUPDATE;
		//searchops += fbl->FBESEARCH;
	}

	cout << "\n======== SUMMARY OF METADATA OPEARTIONS ==========\n";

	cout << "\n======== SUMMARY OF METADATA OPEARTIONS ==========\n";
	cout << "INSERT	\t\t\t:" << insertops << endl;
	cout << "DELETE	\t\t\t:" << deleteops << endl;
	cout << "UPDATE	\t\t\t:" << updateops << endl;
	cout << "SEARCH	\t\t\t:" << searchops << endl;
	cout << "\t\t Total\t:" << (insertops + deleteops + updateops + searchops) << endl;

	cout << "\n======== SUMMARY OF METADATA OVERHEADS  ===========\n";
	cout << "read count for read operations \t:" << readOpReads << endl;
	cout << "write count for read operations \t:" << readOpWrites << endl;
	cout << "read count for write operations \t:" << writeOpReads << endl;
	cout << "write count for write operations \t:" << writeOpWrites << endl;
	cout << "\n===================================================\n";
	cout << readOpReads / DATA_BLOCK_READ_COUNT << " reads, " 
		<< readOpWrites / DATA_BLOCK_READ_COUNT << " writes / Data block read\n";
	cout << writeOpReads / DATA_BLOCK_WRITE_COUNT << " reads, " 
		<< writeOpWrites / DATA_BLOCK_WRITE_COUNT << " writes / Data block written\n";
}

MDMaps::~MDMaps(void) {
	flushCache();
}

