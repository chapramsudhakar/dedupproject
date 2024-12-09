#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "fblist.h"
#include "cache.h"

void FBList::kvdelete(unsigned int i, FBEntryIncore *c) {
	if (c->next != NULL) (c->next)->prev = c->prev;
	if (c->prev != NULL) (c->prev)->next = c->next;
	else KVQ[i] = c->next;

	if (c->tnext != NULL) (c->tnext)->tprev = c->tprev;
	else KVTail = c->tprev;
	if (c->tprev != NULL) (c->tprev)->tnext = c->tnext;
	else KVHead = c->tnext;
}

void FBList::kvprepend(unsigned int i, FBEntryIncore *c) {
	c->prev = NULL;
	c->next = KVQ[i];
	if (KVQ[i] != NULL) KVQ[i]->prev = c;
	KVQ[i] = c;

	c->tprev = NULL;
	c->tnext = KVHead;
	if (KVHead != NULL) {
		KVHead->tprev = c;
		KVHead = c;
	}
	else KVTail = KVHead = c;
}

FBEntryIncore * FBList::kvsearch(unsigned int i, unsigned int key) {
	FBEntryIncore  *c;

	c = KVQ[i];
	while ((c != NULL) && (c->key != key))
		c = c->next;
	return c;
}

FBEntryIncore * FBList::kvalloc(void) {
	FBEntryIncore  *c = NULL;
	int i;

	if (FL != NULL) {
		// Allocate from free list
		c = FL;
		FL = FL->next;
	}
	else {
		// Replace an existing entry at the tail end
		c = KVTail;

		i = c->key % KVHASH;
		kvdelete(i, c);
		if (c->flags & CACHE_ENTRY_DIRTY) {
			fbeWrite(c);
			FBEUPDATE++;
		}
	}

	return c;
}

void FBList::fbeRead(unsigned int n, FBEntryIncore *p) {
	checkSeekPos(n);
	fbref.seekg((unsigned long)(n-1) * sizeof(struct FBEntry));
	if (fbref.fail()) {
		cout << "Error: fbeRead seek failed\n";
		fbref.clear();
		return;
	}

	fbref.read((char *)&p->pba, sizeof(unsigned int));
	fbref.read((char *)&p->snode, sizeof(unsigned int));
	fbref.read((char *)&p->nb, sizeof(unsigned int));
	fbref.read((char *)&p->enext, sizeof(unsigned int));
	p->key = n;

	FBESEARCH++;
}

void FBList::fbeWrite(FBEntryIncore *p) {
	checkSeekPos(p->key);
	fbref.seekp((unsigned long)(p->key-1) * sizeof(struct FBEntry));
	if (fbref.fail()) {
		cout << "Error: fbeWrite seek failed\n";
		fbref.clear();
		return;
	}

	fbref.write((char *)&p->pba, sizeof(unsigned int));
	fbref.write((char *)&p->snode, sizeof(unsigned int));
	fbref.write((char *)&p->nb, sizeof(unsigned int));
	fbref.write((char *)&p->enext, sizeof(unsigned int));
}

FBList::FBList(void) {
	FBE_CACHE_HIT = 0;
	FBE_CACHE_MISS = 0;
	FBESEARCH = 0;
	FBEINSERT = 0;
	FBEDELETE = 0;
	FBEUPDATE = 0;
}

FBList::FBList(const char *name, unsigned int csize) {
	strcpy(fbname, name);
	fbref.open(fbname, ios::in | ios::out | ios::binary);

	FBE_CACHE_HIT = 0;
	FBE_CACHE_MISS = 0;
	FBESEARCH = 0;
	FBEINSERT = 0;
	FBEDELETE = 0;
	FBEUPDATE = 0;

	fbeCacheInit(csize);
}


void FBList::fbeCacheInit(unsigned int size) {
	unsigned int i, n;
	struct FBEntryIncore *p, *q;

	KVCacheSize = size;
	n = KVCacheSize / sizeof(struct FBEntryIncore);
	q = (struct FBEntryIncore *) malloc(n * sizeof(struct FBEntryIncore));
	if (q == NULL) {
		cout << " FB Entry Cache malloc 1 failure" << endl;
		exit(0);
	}

	p = FL = q;
	p->prev = NULL;
	for (i=1; i<n; i++) {
		p->next = &q[i];
		p = p->next;
	}
	p->next = NULL;

	KVHASH = n/20 + 1;
	KVQ = (struct FBEntryIncore **) malloc(KVHASH * sizeof(struct FBEntryIncore *));
	if (KVQ == NULL) {
		cout << " FB Entry Cache malloc 2 failure" << endl;
		exit(0);
	}

	for (i=0; i<KVHASH; i++) KVQ[i] = NULL;
	KVTail = KVHead = NULL;

	fbref.seekg(0, fbref.end);
	maxb = fbref.tellg() / sizeof(struct FBEntry);
}

void FBList::checkSeekPos(unsigned int ent) {
	int i;
	struct FBEntry p;
	//struct FBEntryIncore *p1;
	unsigned int nb, pba, snode, next, first, last;


	if (maxb < ent) {
		fbeGetEntry(1, &pba, &snode, &nb, &next);
		// Expand the pbaref file
		fbref.seekp(0, fbref.end);
		memset(&p, 0, sizeof(struct FBEntry));
		first = maxb+1;
		while (maxb < ent) {
			for (i=1; i<(1024*1024); i++) {
				p.fbe_next = maxb + i + 1;
				fbref.write((char *)&p, sizeof(struct FBEntry));
			}
			// Last entry
			p.fbe_next = 0;
			fbref.write((char *)&p, sizeof(struct FBEntry));

			maxb += (1024*1024);	
		}
		last = maxb;
		if (nb != 0)
			fbePutEntry(nb, 0, 0, 0, first);
		else fbePutEntry(1, 0, 0, last, first);
	}
}

void FBList::fbeGetEntry(unsigned int n, unsigned int *pba,
		unsigned int *snode, unsigned int *nb, unsigned int *next) {
	struct FBEntryIncore *p; 
	unsigned int i;

	i = n % KVHASH;
	p = kvsearch(i, n);

	if (p != NULL) {
		// Entry found in the cache
		FBE_CACHE_HIT++;
		kvdelete(i, p);
		*pba = p->pba;
		*snode = p->snode;
		*nb = p->nb;
		*next = p->enext;
		kvprepend(i, p);
	}
	else {
		FBE_CACHE_MISS++;
		p = kvalloc();

		fbeRead(n, p);

		*pba = p->pba;
		*snode = p->snode;
		*nb = p->nb;
		*next = p->enext;

		p->flags = CACHE_ENTRY_CLEAN;

		kvprepend(i, p);
	}
	return;
}

void FBList::fbePutEntry(unsigned int n, unsigned int pba, 
		unsigned int snode, unsigned int nb, unsigned int next) {
	struct FBEntryIncore *p; 
	//unsigned int retval = 0;
	unsigned int i;

	i = n % KVHASH;
	p = kvsearch(i, n);
	if (p != NULL) {
		FBE_CACHE_HIT++;
		
		// Entry found in the cache
		p->pba = pba;
		p->snode = snode;
		p->nb = nb;
		p->enext = next;
		p->flags |= CACHE_ENTRY_DIRTY;
	}
	else {
		// Otherwise read the entry from 
		// the file
		FBE_CACHE_MISS++;
		
		p = kvalloc();

		p->key = n;
		p->pba = pba;
		p->snode = snode;
		p->nb = nb;
		p->enext = next;
		p->flags = CACHE_ENTRY_DIRTY;
	}

	return;
}

unsigned int FBList::getFreeFbe(void) {
	unsigned int ent = 0;
	unsigned int pbax, pba, nbx, nb, snodex, snode, next;

	while (ent == 0) {
		fbeGetEntry(1, &pbax, &snodex, &nbx, &next);
		if (next != 0) {
			ent = next;
			fbeGetEntry(ent, &pba, &snode, &nb, &next);
			if (next == 0) nbx = 0;
			fbePutEntry(1, pbax, snodex, nbx, next);
		}
		else checkSeekPos(maxb+1);
	}	

	return ent;
}

void FBList::freeFbe(unsigned int n) {
	//unsigned int ent = 0;
	unsigned int pbax, snodex, nbx, next;

	fbeGetEntry(1, &pbax, &snodex, &nbx, &next);
	if (next != 0) {
		fbePutEntry(n, 0, 0, 0, next);
		fbePutEntry(1, pbax, snodex, nbx, n);
	}
	else {
		fbePutEntry(n, 0, 0, 0, 0);
		fbePutEntry(1, pbax, snodex, next, next);
	}
}

unsigned int FBList::flushCache(void) {
	return kvflush();
}

unsigned int FBList::kvflush(void) {
	struct FBEntryIncore *c;
	unsigned int count = 0;

	c = KVTail;
	while (c != NULL) {
		if (c->flags & CACHE_ENTRY_DIRTY) {
			fbeWrite(c);
			FBEUPDATE++;
			c->flags ^= CACHE_ENTRY_DIRTY;
			count++;
		}

		c = c->tprev;
	}
	return count;
}

