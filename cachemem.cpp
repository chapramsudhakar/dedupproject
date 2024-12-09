#include <iostream>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

using namespace std;

#include "cachemem.h"
//#include "metadata.h"

//struct CacheEntry 	*cem, *CEMBLOCK;
//struct WriteReqBuf	*wrbm, *WRBMBLOCK;

//struct mblock 	*cemslabs;

//unsigned int	cemcount = 0;
//unsigned int	b512mcount = 0;
//unsigned int	wrbmcount = 0;
//unsigned int 	CEMSLABCOUNT = 0;

//char	*b512mblocks;
//struct b512fl	*fl;
//extern MDMaps *md;
//static long long int alloccount = 0, freecount = 0;

void CacheMem::cacheMemStats(void) {
	cout << "\n\nCacheType \tSize(KB) \t#Max \t#Used\n";
	cout << "CacheEntry \t" << (CEMSLABCOUNT * CEMSLABSIZE)/1024 << "\t\t"
		<< (CEMSLABCOUNT * CEMSLABSIZE) / sizeof(struct CacheEntry) 
		<< "\t" << cemcount << endl
		<< "Block 512\t--\t\t--\t" 
		<< b512mcount << endl
		<< "wrbuf\t\t" << (WRB_CACHE_SIZE) / 1024 << "\t--\t\t"
		<< WRBMCOUNT << "\t" << wrbmcount << endl;
	cout << "alloccount = " << alloccount << " free count " << freecount << endl;
}

void CacheMem::initCacheMem(const char *name) {
	unsigned int i;
	struct WriteReqBuf	* t = NULL;
	struct b512fl *p;

	strcpy(cachememname, name);

	cemcount = 0;
	b512mcount = 0;
	wrbmcount = 0;
	CEMSLABCOUNT = 0;
	alloccount = 0;
	freecount = 0;

	// Initialize wrbm cache
	wrbm = t = (struct WriteReqBuf *)malloc(WRBMCOUNT * sizeof(struct WriteReqBuf));
	assert(wrbm != NULL);

	for (i=1; i<WRBMCOUNT; i++) {
		t->wr_tsnext = &wrbm[i];
		t = t->wr_tsnext;
	}
	t->wr_tsnext = NULL;

	// Create / extend CacheEntries
	cemslabs = NULL;
	createCacheEntries();

	b512mblocks = (char *) malloc(B512MCOUNT * 512);
	//perror("after malloc:");
	assert(b512mblocks != NULL);

	//cout << cachememname << "::cachemem init : b512 " << B512MCOUNT << endl;
	//cout << "bkt cache size : " << BUCKET_CACHE_SIZE / 512 << endl;
	//cout << "hbkt cache size : " << HBUCKET_CACHE_SIZE / 512 << endl;
	//cout << "blkidx cache size3 : " << BLOCKINDEX_CACHE_SIZE3 / 512 << endl;
	//cout << "blkidx cache size4 : " << BLOCKINDEX_CACHE_SIZE4 / 512 << endl;
	//cout << "bucketidx cache size4 : " << BUCKETINDEX_CACHE_SIZE / 512 << endl;
	fl = p = (struct b512fl *)b512mblocks;

	for (i=1; i<B512MCOUNT; i++) {
		p->next = (struct b512fl *) &b512mblocks[i*512];
		p = p->next;
	}
	p->next = NULL;
	//cout << "init cache mem : b12mcount " << B512MCOUNT << endl
		//<< " addr " << static_cast<void *>(b512mblocks) << endl;
	// 512 byte blocks are dynamically allocated using malloc/free calls.

}

WriteReqBuf *CacheMem::wrbmmalloc() {
	WriteReqBuf *p;

	if (wrbm == NULL) return NULL;
	else {
		p = wrbm;
		wrbm = wrbm->wr_tsnext;
		wrbmcount++;
		return p;
	}
}

void CacheMem::wrbmfree(WriteReqBuf *c) {
	WriteReqBuf *p;

	p = (WriteReqBuf *)c;
	p->wr_tsnext = wrbm;
	wrbm = p;
	wrbmcount--;
}

void CacheMem::createCacheEntries(void) {
	CacheEntry *c1, *c2;
	mblock * mb1; 
	unsigned int i;

	// Allocate one block and prepend it to cemslabs
	mb1 = (mblock *) malloc(sizeof(mblock));
	assert(mb1 != NULL);
	CEMSLABCOUNT++;

	mb1->mb_next = cemslabs;
	mb1->mb_prev = NULL;
	if (cemslabs != NULL) cemslabs->mb_prev = mb1;
	cemslabs = mb1;
	mb1->mb_nactive = 0;
	mb1->mb_ptr = (char *)malloc(CEMSLABSIZE);
	assert(mb1->mb_ptr != NULL);

	mb1->mb_flist = c1 = c2 = (CacheEntry *)mb1->mb_ptr;
	for (i=1; i<(CEMSLABSIZE / sizeof(CacheEntry)); i++) {
		c1->ce_next = &c2[i];
		c1 = c1->ce_next;
	}
	c1->ce_next = NULL;
	//cemcount += (CEMSLABSIZE / sizeof(CacheEntry));
}

CacheEntry *CacheMem::cemalloc() {
	struct CacheEntry *p = NULL;
	mblock	*mb;

	// Allocate from temporary list
	if (cem != NULL) {
		p = cem;
		cem = cem->ce_next;
		cemcount--;
	}

	// Temporary list is empty
	if (p == NULL) {
		// Allocate from cemslabs
		//cout << "cemalloc 1: " << cemcount << endl;
		mb = cemslabs;
		while (mb != NULL) {
			p = mb->mb_flist;
			if (p != NULL) {
				//cout << "cemalloc 2: " << p << endl;
				mb->mb_flist = p->ce_next;
				mb->mb_nactive++;
				break;
			}
			else mb = mb->mb_next;
		}
	}

	// If free node is not found, expand the list by
	// creating new nodes
	if (p == NULL) {
		//cout << "cemalloc 3: " << p << endl;
		createCacheEntries();
		// If successful, then definitely at first block
		// non-empty free list can be found
		mb = cemslabs;
		p = mb->mb_flist;
		//cout << "cemalloc 4: " << p << endl;
		mb->mb_flist = p->ce_next;
		mb->mb_nactive++;
	}

	memset(p, 0, sizeof(struct CacheEntry));
	return p;
}

// Return entry to temporary free list
void CacheMem::cefree(CacheEntry *c) {
	c->ce_next = cem;
	cem = c;
	cemcount++;
}

// In the background, garbage collector can be
// called, if more entries are free
void CacheMem::cegb(void) {
	CacheEntry *c, *c1;;
	mblock *mb1;
	int done;
	char *cptr;

	c = cem;

	while (c != NULL) {
		c1 = c->ce_next;

		// Find the mblock, to which c belongs to.
		mb1 = cemslabs;
		done = 0;
		cptr = (char *)c;
		while (!done) {
			if ((mb1->mb_ptr <= cptr) && 
				(cptr < (mb1->mb_ptr + CEMSLABSIZE))) {
				c->ce_next = mb1->mb_flist;
				mb1->mb_flist = c;
				mb1->mb_nactive--;
				done = 0;
			}
			else mb1 = mb1->mb_next;
		}

		c = c1;
	}
}
//extern long long int bktalloc, bktfree, btalloc, btfree, bktfree, hbktalloc, hbktfree, hbtalloc, hbtfree;

char *CacheMem::b512malloc() {
	char *p;

	//p = (char *)malloc(512);
	//assert(p != NULL);
	//return p;

	alloccount++;
	p = (char *)fl;
	if (p == NULL) {
		cout << cachememname << "::b512malloc failure - b512mcount " << b512mcount << " max " << B512MCOUNT << endl;
		//cout << "Computed val " << (((BUCKET_CACHE_SIZE + HBUCKET_CACHE_SIZE + 2 * BLOCKINDEX_CACHE_SIZE3 + BUCKETINDEX_CACHE_SIZE))) << endl;
		cout << "alloccount = " << alloccount << " free count " << freecount << endl;
		//md->displayStatistics();
		//cout << "bktalloc " << bktalloc 
			//<< " bktfree " << bktfree << endl;
		//cout << "hbktalloc " << hbktalloc 
			//<< " hbktfree " << hbktfree << endl;
		//cout << "btalloc " << btalloc 
			//<< " btfree " << btfree << endl;
		//cout << "hbtalloc " << hbtalloc 
			//<< " hbtfree " << hbtfree << endl;
		assert(p != NULL);
	}
	fl = fl->next;
	b512mcount++;
	//cout << cachememname << "::b512malloc " << static_cast<void *>(p) << " b512mcount " << b512mcount << endl;
	return p;
}

void CacheMem::b512free(char *c) {
	struct b512fl	*p;

	//free(c);
	//return;

	freecount++;
	p = (struct b512fl *) c;
	p->next = fl;
	fl = p;
	b512mcount--;
	//cout << cachememname << "::b512free " << static_cast<void *>(c) << " b512mcount " << b512mcount << endl;
}

/*
void *b512malloc() {
        struct block512Mem *p;

        if (b512mp == NULL) return NULL;
        else {
                p = b512mp;
                b512mp = b512mp->m_next;
                b512mcount++;
                return p;
        }
}

void b512free(void *c) {
        struct block512Mem *p;

        p = (struct block512Mem *)c;
        p->m_next = b512mp;
        b512mp = p;
        b512mcount--;
}
*/
