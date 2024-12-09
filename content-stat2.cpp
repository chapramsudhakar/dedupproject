#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include "dedupconfig.h"
#include "dedupn.h"
#include "iotracereader.h"
#include "dupset.h"

#define HASH_SIZE	200000
struct HVertex {
	unsigned char	hash[16];
	struct HVertex *next;
	struct HVertex *prev;
};

struct V {
	unsigned int lba;
	struct V *next;
	struct V *prev;
};

struct V * vlist[HASH_SIZE];
struct HVertex * hlist[HASH_SIZE];

int md5hashIndex(unsigned char * h) {
	unsigned int x;

	memcpy(&x, h, sizeof(unsigned int));
	return (x % HASH_SIZE);
}

int searchV(struct V *l[], unsigned int lba) {
	struct V *v;
	int i;

	i = (lba/8) % HASH_SIZE;

	// Check if present
	v = l[i];
	while ((v != NULL) && (v->lba != lba)) v = v->next;
	if (v != NULL) return 1;
	else return 0;
}

void addV(struct V * l[], unsigned int lba) {
	struct V *v;
	int i;

	i = (lba/8) % HASH_SIZE;

	v = (struct V*)malloc(sizeof(struct V));
	v->lba = lba;

	v->prev = NULL;
	v->next =  l[i];
	if (l[i] != NULL) {
		l[i]->prev = v;
	}
	l[i] = v;
}

void displayV(void) {
	struct V *v;
	struct HVertex *hv;
	int count = 0;
	int i;

	for (i=0; i<HASH_SIZE; i++) {
		v = vlist[i];
		while (v != NULL) {
			count++;
			v = v->next;
		}
	}

	cout << "Lba count \t\t\t: " << count << endl;

	count = 0;
	for (i=0; i<HASH_SIZE; i++) {
		hv = hlist[i];
		while (hv != NULL) {
			count++;
			hv = hv->next;
		}
	}

	cout << "Different contents written count \t\t\t: " << count << endl;
}

struct HVertex * addHVertex(unsigned char * hash) {
	struct HVertex *v;
	int i;

	i = md5hashIndex(hash);

	v = (struct HVertex*)malloc(sizeof(struct HVertex));
	memcpy(v->hash, hash, 16);

	v->prev = NULL;
	v->next =  hlist[i];
	if (hlist[i] != NULL) {
		hlist[i]->prev = v;
	}
	hlist[i] = v;

	return v;
}

struct HVertex *findHVertex(unsigned char * hash) {
	struct HVertex *v; //, *v1;
	int i;

	i = md5hashIndex(hash);

	v = hlist[i];

	while (v != NULL) {
		if (memcmp(hash, v->hash, 16) == 0) 
			return v;
		else v = v->next;
	}

	return NULL;
}

char fnames[][100] = {

/*
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.1.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.2.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.3.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.4.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.5.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.6.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.7.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.8.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.9.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.10.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.11.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.12.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.13.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.14.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.15.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.16.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.17.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.18.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.19.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.20.blkparse",
	"/Volumes/HD-E1/Godavari/mail-01/cheetah.cs.fiu.edu-110108-113008.21.blkparse"
*/
/*
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.2.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.3.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.4.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.5.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.6.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.7.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.8.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.9.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.10.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.11.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.12.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.13.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.14.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.15.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.16.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.17.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.18.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.19.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.20.blkparse",
"/Volumes/HD-E1/Godavari/Web-VM/webmail+online.cs.fiu.edu-110108-113008.21.blkparse"
*/

/*
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.1.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.2.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.3.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.4.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.5.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.6.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.7.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.8.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.9.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.10.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.11.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.12.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.13.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.14.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.15.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.16.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.17.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.18.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.19.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.20.blkparse",
"/Volumes/HD-E1/Godavari/Homes/homes-110108-112108.21.blkparse"
*/

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
 "../../../../Homes/homes-110108-112108.1.blkparse",
 "../../../../Homes/homes-110108-112108.2.blkparse",
 "../../../../Homes/homes-110108-112108.3.blkparse",
 "../../../../Homes/homes-110108-112108.4.blkparse",
 "../../../../Homes/homes-110108-112108.5.blkparse",
 "../../../../Homes/homes-110108-112108.6.blkparse",
 "../../../../Homes/homes-110108-112108.7.blkparse",
 "../../../../Homes/homes-110108-112108.8.blkparse",
 "../../../../Homes/homes-110108-112108.9.blkparse",
 "../../../../Homes/homes-110108-112108.10.blkparse",
 "../../../../Homes/homes-110108-112108.11.blkparse",
 "../../../../Homes/homes-110108-112108.12.blkparse",
 "../../../../Homes/homes-110108-112108.13.blkparse",
 "../../../../Homes/homes-110108-112108.14.blkparse",
 "../../../../Homes/homes-110108-112108.15.blkparse",
 "../../../../Homes/homes-110108-112108.16.blkparse",
 "../../../../Homes/homes-110108-112108.17.blkparse",
 "../../../../Homes/homes-110108-112108.18.blkparse",
 "../../../../Homes/homes-110108-112108.19.blkparse",
 "../../../../Homes/homes-110108-112108.20.blkparse",
 "../../../../Homes/homes-110108-112108.21.blkparse"
/*
 */

/*
	 "../../../../Homes/homes-110108-112108.15.blkparse"
	"../../../../mail-01/sample-10000"
	"../../../../Web-VM/webmail+online.cs.fiu.edu-110108-113008.1.blkparse",
*/


};
unsigned int segs[2049];
unsigned int seglen[2049];
int main(int argc, char** argv) {
		
	IOTraceReader *trace;
#ifdef	HOME
	struct segmenthome s;
#else
	struct segment4k s;
#endif
	int count = 0;
	int rcount = 0, wcount = 0;
	int pid, op;
	unsigned int i; 
	unsigned long long reccount100000 = 1;
	unsigned long long int ts1, ts2;

#ifdef	HOME
	trace = new IOTraceReaderHome();
	//if (trace.openTrace(fnames, 7) != 0) {
	if (trace->openTrace(fnames, 21, 512) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#else
	trace = new IOTraceReader4K();
	//if (trace.openTrace(fnames, 7) != 0) {
	if (trace->openTrace(fnames, 21, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#endif


	while (trace->readNextSegment(&pid, &op, &ts1, &ts2, &s)) {
#ifndef	HOME
		if (op == OP_READ) 
			rcount += (s.seg_size * 8);
		else 	wcount += (s.seg_size * 8);
#else
		if (op == OP_READ) 
			rcount += (s.seg_size);
		else 	wcount += (s.seg_size);
#endif

#ifndef	HOME
		for (i=0; i<s.seg_size; i++) {
			if (searchV(vlist, s.seg_start + (i * 8)) == 0)
				addV(vlist, s.seg_start + (i * 8));

			if (findHVertex(s.seg_hash[i]) == NULL) 
				addHVertex(s.seg_hash[i]);
		}
#else
		for (i=0; i<s.seg_size; i++) {
			if (searchV(vlist, s.seg_start + i) == 0)
				addV(vlist, s.seg_start + i);

			if (findHVertex(s.seg_hash[i]) == NULL) 
				addHVertex(s.seg_hash[i]);
		}
#endif

		if (trace->getCurCount() >= (reccount100000 * 500000)) {
			cout << trace->getCurCount() << "\t number of records processed\n";
			reccount100000++;
		}
	}

	count = trace->getCurCount();			
	trace->closeTrace();	

	cout << "\n" << count << " number of records, read \t= " << rcount << "  write \t= " << wcount << endl << endl;
	displayV();

	return 0;
}

