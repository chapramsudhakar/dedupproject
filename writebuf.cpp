
// Delayed write buffer management
#include <iostream>
#include <stdlib.h>

using namespace std;
#include "writebuf.h"

#define	WRHASH_SIZE	(WRBMCOUNT/20)

WriteReqBuf	*tshead, *tstail, *lbahead, *lbatail;
WriteReqBuf	*hashhead[WRHASH_SIZE], *hashtail[WRHASH_SIZE];

double tsLen = 0, tsCount = 0, hashLen = 0, hashCount = 0;
double lbaLen = 0, lbaCount = 0, lbaInsertLen = 0, lbaInsertCount = 0;

void displayWriteReqBufMemStat() {
	cout << "\nTS count : " << tsCount << " Avg TS Len : " << tsLen / tsCount
		<< "\nHash count : " << hashCount << " Avg Hash Len : " 
		<< hashLen / hashCount << " lba count " << lbaCount 
		<< " lba avg len : " << lbaLen / lbaCount 
		<< "\nLba insert count : " << lbaInsertCount
		<< " avg lba insert len " << lbaInsertLen / lbaInsertCount << endl << endl;
}

void insertWriteReqTSHead(WriteReqBuf *wreq) {
	// For the read requests, whose corresponding writes
	// were not available in the traces, generate a new write request
	// with old time stamp as that of oldest one in the queue and 
	// insert at the beginning
	
	wreq->wr_tsprev = NULL;
	wreq->wr_tsnext = tshead;
	if (tshead != NULL) {
		tshead->wr_tsprev = wreq;
		wreq->wr_ts = ((tshead->wr_ts - 1) > 0) ? tshead->wr_ts - 1 : 1;
	}
	else {
		tstail = wreq;
		wreq->wr_ts = 1;
	}
	tshead = wreq;
	tsLen += 1;
	tsCount += 1;
}

void insertWriteReqTS(WriteReqBuf * wreq) {
	// Assuming that the requests are sent in time order
	// for insertion, So inserted at the end always.
	wreq->wr_tsnext = NULL;
	wreq->wr_tsprev = tstail;
	if (tstail != NULL) 
		tstail->wr_tsnext = wreq;
	else
		tshead = wreq;
	tstail = wreq;
	tsLen += 1;
	tsCount += 1;
}

void deleteWriteReqTS(WriteReqBuf *wreq) {
	if (wreq->wr_tsprev == NULL) 
		tshead = wreq->wr_tsnext;
	else
		(wreq->wr_tsprev)->wr_tsnext = wreq->wr_tsnext;

	if (wreq->wr_tsnext == NULL) 
		tstail = wreq->wr_tsprev;
	else
		(wreq->wr_tsnext)->wr_tsprev = wreq->wr_tsprev;
}

void insertWriteReqHASH(WriteReqBuf *wreq) {
	int i;

	i = wreq->wr_lba % WRHASH_SIZE;

	// Inserted at the beginning of hash queue
	wreq->wr_hashnext = hashhead[i];
	wreq->wr_hashprev = NULL;

	if (hashhead[i] != NULL) 
		hashhead[i]->wr_hashprev = wreq;
	else
		hashtail[i] = wreq;

	hashhead[i] = wreq;
	hashCount += 1;
	hashLen += 1;
}

void deleteWriteReqHASH(WriteReqBuf *wreq) {
	int i;

	i = wreq->wr_lba % WRHASH_SIZE;

	if (wreq->wr_hashprev == NULL) 
		hashhead[i] = wreq->wr_hashnext;
	else
		(wreq->wr_hashprev)->wr_hashnext = wreq->wr_hashnext;

	if (wreq->wr_hashnext == NULL) 
		hashtail[i] = wreq->wr_hashprev;
	else
		(wreq->wr_hashnext)->wr_hashprev = wreq->wr_hashprev;
}

WriteReqBuf * searchWriteReq(unsigned int lba) {
	WriteReqBuf *t1;
	int i;

	i = lba % WRHASH_SIZE;
	t1 = hashhead[i];

	lbaCount += 1;
	while (t1 != NULL) {
		if (t1->wr_lba == lba) return t1;
		else t1 = t1->wr_hashnext;
		lbaLen++;
	}

	return NULL;
}

WriteReqBuf * searchWriteReqNear(unsigned int lba) {
	WriteReqBuf *t1, *t2;
	int i, j, diff;
	unsigned int mindiff1, mindiff2;

	i = lba % WRHASH_SIZE;
	
	t1 = t2 = hashhead[i];

	j = (i+1) % WRHASH_SIZE;	
	while ((t1 == NULL) && (j != i)) {
		t1 = t2 = hashhead[j];
		j = (j+1) % WRHASH_SIZE;	
		lbaInsertLen++;
	}

	if (t1 != NULL) {
		diff = t1->wr_lba - lba;
		mindiff1 = abs((diff));
	}

	while (t1 != NULL) {
		if (t1->wr_lba == lba) return t1;
		else {
			diff = t1->wr_lba - lba;
			mindiff2 = abs(diff);
			if (mindiff2 < mindiff1) {
				t2 = t1;
				mindiff1 = mindiff2;
			}
		}
		t1 = t1->wr_hashnext;
		lbaInsertLen++;
	}

	return t2;
}

static WriteReqBuf *lastReq = NULL;

void insertWriteReqLBA(WriteReqBuf *wreq) {
	WriteReqBuf *t1, *t2;
	int diff1, diff2;

	lbaInsertCount++;
	if (lbahead == NULL) {
		wreq->wr_lbanext = NULL;
		wreq->wr_lbaprev = NULL;
		lbahead = lbatail = wreq;
		lastReq = wreq;
		return;
	}

	if ((lastReq != NULL) && ((lastReq->wr_lba + 1) == wreq->wr_lba))
		t1 = lastReq;
	else
		t1 = searchWriteReqNear(wreq->wr_lba);

	if (t1 == NULL) {
		diff1 = lbahead->wr_lba - wreq->wr_lba;
		diff2 =	lbatail->wr_lba - wreq->wr_lba;
		if (abs(diff1) < abs(diff2))
			t1 = lbahead;
		else
			t1 = lbatail;
	}

	if (t1->wr_lba < wreq->wr_lba) {
		// Successor
		t2 = t1;
		t1 = t1->wr_lbanext;

		while ((t1 != NULL) && (t1->wr_lba < wreq->wr_lba)) {
			lbaInsertLen++;
			t2 = t1;
			t1 = t1->wr_lbanext;
		}

		// Must be inserted between t2 and t1
		wreq->wr_lbanext = t1;
		wreq->wr_lbaprev = t2;
		t2->wr_lbanext = wreq;
		if (t1 != NULL) t1->wr_lbaprev = wreq;
		else lbatail = wreq;
	}
	else {
		// Predecessor
		t2 = t1;
		t2 = t2->wr_lbaprev;

		while ((t2 != NULL) && (t2->wr_lba > wreq->wr_lba)) {
			lbaInsertLen++;
			t1 = t2;
			t2 = t2->wr_lbaprev;
		}

		// Must be inserted between t2 and t1
		wreq->wr_lbanext = t1;
		wreq->wr_lbaprev = t2;
		t1->wr_lbaprev = wreq;
		if (t2 != NULL) t2->wr_lbanext = wreq;
		else lbahead = wreq;
	}

	lastReq = wreq;
}

void deleteWriteReqLBA(WriteReqBuf *wreq) {
	if (wreq->wr_lbaprev == NULL) 
		lbahead = wreq->wr_lbanext;
	else
		(wreq->wr_lbaprev)->wr_lbanext = wreq->wr_lbanext;

	if (wreq->wr_lbanext == NULL) 
		lbatail = wreq->wr_lbaprev;
	else
		(wreq->wr_lbanext)->wr_lbaprev = wreq->wr_lbaprev;

	if (lastReq == wreq) lastReq = NULL;
}

