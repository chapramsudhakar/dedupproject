#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include "dedupconfig.h"
#include "dedupn.h"
#include "iotracereader.h"
#include "dupset.h"

#define HASH_SIZE	20000
struct HVertex {
	unsigned char	hash[16];
	unsigned int lba;
	struct HVertex *next;
	struct HVertex *prev;
};

struct V {
	unsigned int lba;
	struct V *next;
	struct V *prev;
};

struct V *rlist[HASH_SIZE];
struct V *wlist[HASH_SIZE];
struct V *rwlist[HASH_SIZE];

struct Vertex * vlist[HASH_SIZE];
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
	int count1 = 0, count2 = 0, count3 = 0;
	int i;

	for (i=0; i<HASH_SIZE; i++) {
		v = rlist[i];
		while (v != NULL) {
			count1++;
			v = v->next;
		}
		v = wlist[i];
		while (v != NULL) {
			count2++;
			v = v->next;
		}
		v = rwlist[i];
		while (v != NULL) {
			count3++;
			v = v->next;
		}
	}

	cout << "Read requests (lba's) \t\t\t: " << count1 << endl
		<< "Write requests (lba's) \t\t\t: " << count2 << endl
		<< "Read and write requests (lba's) \t: " << count3 << endl;
}

struct Vertex * addVertex(unsigned int lba, unsigned int par) {
	struct Vertex *v;
	int i;

	i = (lba/8) % HASH_SIZE;

	v = (struct Vertex*)malloc(sizeof(struct Vertex));
	v->lba = lba;
	v->par = par;

	v->hprev = NULL;
	v->hnext =  vlist[i];
	if (vlist[i] != NULL) {
		vlist[i]->hprev = v;
	}
	vlist[i] = v;

	return v;
}

struct Vertex *findVertex(unsigned int lba) {
	struct Vertex *v, *v1;
	int i;

	i = (lba/8) % HASH_SIZE;

	v = vlist[i];

	while (v != NULL) {
		if (v->lba == lba) {
			if (v->lba == v->par) // Final representative
				return v;
			else {
				// Find the representative vertex
				v1 = findVertex(v->par);
				v->par = v1->lba;
				return v1;
			}
		}
		else {
			v = v->hnext;
		}
	}

	return NULL;
}

struct HVertex * addHVertex(unsigned char * hash, unsigned int lba) {
	struct HVertex *v;
	int i;

	i = md5hashIndex(hash);

	v = (struct HVertex*)malloc(sizeof(struct HVertex));
	memcpy(v->hash, hash, 16);
	v->lba = lba;

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

void displayDupSet(void) {
	struct Vertex *v;
	struct HVertex *hv;
	int i;
	int count1 = 0, count2 = 0, count3=0;

	for (i=0; i<HASH_SIZE; i++) {
		v = vlist[i];

		while (v != NULL) {
			if (v->lba == v->par) // Final representative
				count1++;
			else {
				cout << v->par << "\t" << v->lba <<endl;
				count3++;
			}
			v = v->hnext;
		}
	}
	cout << "+++++++++++++++++++++++++++++\n";
	
	for (i=0; i<HASH_SIZE; i++) {
		hv = hlist[i];
		while (hv != NULL) {
			cout << hv->lba << endl;
			hv = hv->next;
			count2++;
		}
	}
	cout << "Count1 = " << count1 << "  Count2 = " << count2 
		<< " count3 = " << count3 << endl;
	
}

char fnames[][100] = {
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
/*
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
/*
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
	IORecordHome rec;			// I/O record
	IORecordHome rec1;
#else
	IORecord4K rec;			// I/O record
	IORecord4K rec1;
#endif
	int count = 0;
	int rcount = 0, wcount = 0;
	int flag = 0;
	int len = 0;
	unsigned int i, j, t;
	float segwcount = 0.0F, segsum = 0.0F;

#ifdef	HOME
	trace = new IOTraceReaderHome();
	if (trace->openTrace(fnames, 21, 512) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#else
	trace = new IOTraceReader4K();
	if (trace->openTrace(fnames, 21, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
#endif

	while (trace->readNext(rec)) {
		if (rec.ior_rw == 'R') {
			rcount++;
			if (searchV(rlist, rec.ior_lba) == 0)
				addV(rlist, rec.ior_lba);
			if ((searchV(wlist, rec.ior_lba) == 1) &&
				(searchV(rwlist, rec.ior_lba) == 0))
				addV(rwlist, rec.ior_lba);
		}
		else {
			wcount++;
			if (searchV(wlist, rec.ior_lba) == 0)
				addV(wlist, rec.ior_lba);
			if ((searchV(rlist, rec.ior_lba) == 1) &&
				(searchV(rwlist, rec.ior_lba) == 0))
				addV(rwlist, rec.ior_lba);
		}

		if (flag == 0) {
			rec1 = rec;
			len = 1;
			flag = 1;
		}
		else {
			if ((rec1.ior_pid == rec.ior_pid) &&
				(rec1.ior_lba + (len * 8) == rec.ior_lba) &&
				(rec1.ior_rw == rec.ior_rw))
				len++;
			else {
				if (len <= 2048)
					segs[len-1]++;
				else
					segs[2048]++;

				rec1 = rec;
				len = 1;
			}
		}
		if ((trace->getCurCount() % 10000) == 0)
			cout << trace->getCurCount() << "\tRequests Processed\n";
	}
	count = trace->getCurCount();			
	trace->closeTrace();	

	if (len <= 2048)
		segs[len-1]++;
	else
		segs[2048]++;

	for (i=0; i<2049; i++) seglen[i] = i+1;

	for (i=1; i<2049; i++) {
		for (j=0; j<i-1; j++) {
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
			segwcount += ((i+1) * segs[i]);
			segsum += segs[i];
		}
	}
	cout << "segwcount " << segwcount << " segsum " << segsum 
		<< " Average seg length : " << (segwcount / segsum) << endl;

	cout << "\n" << count << " number of records, read \t= " << rcount << "  write \t= " << wcount << endl << endl;
	//displayDupSet();
	displayV();

	return 0;
}

