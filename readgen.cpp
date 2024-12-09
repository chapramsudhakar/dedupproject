#include <iostream>
#include <iomanip>
#include <fstream>
#include <string.h>
#include <assert.h>
//#include "dedupconfig.h"
//#include "dedupn.h"
#include "iotracereader.h"

#define HASH_SIZE	500000

struct WriteReq {
	unsigned long long int	wr_ts;
	unsigned int		wr_pid;
	//unsigned short		wr_major;
	//unsigned short		wr_minor;
	unsigned int		wr_lba;
	unsigned char		wr_hash[16];
	struct WriteReq		*wr_tsnext;
	struct WriteReq		*wr_tsprev;
	struct WriteReq		*wr_hashnext;
};

WriteReq	*tshead, *tstail;
WriteReq	*hashhead[HASH_SIZE];


void insertWriteReqTS(WriteReq * wreq) {
	// Assuming that the requests are sent in time order
	// for insertion, So inserted at the end always.
	wreq->wr_tsnext = NULL;
	wreq->wr_tsprev = tstail;
	if (tstail != NULL) 
		tstail->wr_tsnext = wreq;
	else
		tshead = wreq;
	tstail = wreq;
}

void deleteWriteReqTS(WriteReq *wreq) {
	if (wreq->wr_tsprev == NULL) 
		tshead = wreq->wr_tsnext;
	else
		(wreq->wr_tsprev)->wr_tsnext = wreq->wr_tsnext;

	if (wreq->wr_tsnext == NULL) 
		tstail = wreq->wr_tsprev;
	else
		(wreq->wr_tsnext)->wr_tsprev = wreq->wr_tsprev;
}

void insertWriteReqHASH(WriteReq *wreq) {
	int i;

	i = wreq->wr_lba % HASH_SIZE;

	// Inserted at the beginning of hash queue
	wreq->wr_hashnext = hashhead[i];
	hashhead[i] = wreq;
}

WriteReq * searchWriteReq(unsigned int lba) {
	WriteReq *t1;
	int i;

	i = lba % HASH_SIZE;
	t1 = hashhead[i];

	while (t1 != NULL) {
		if (t1->wr_lba == lba) return t1;
		else t1 = t1->wr_hashnext;
	}

	return NULL;
}

void outputTrace(const char *fname, int nb) {
	WriteReq *t1;
	ofstream fp(fname);
	unsigned long long int count = 0;
	int i;

	t1 = tshead;

	while (t1 != NULL) {
		//cout << dec << t1->wr_ts << " " << t1->wr_pid << " proc " 
			//<< t1->wr_lba*nb << " " << nb << " W 1 0 ";

		fp << dec << t1->wr_ts << " " << t1->wr_pid << " proc " 
			<< t1->wr_lba*nb << " " << nb << " W 2 3 ";
			//<< t1->wr_major 
			//<< " " << t1->wr_minor << " ";
		for (i=0; i<16; i++) 
			fp << hex << setw(2) << setfill('0') << right << (unsigned int)t1->wr_hash[i];
			//cout << hex << setw(2) << setfill('0') << right << (unsigned int)t1->wr_hash[i];
		fp << dec << endl;
		//cout << dec << endl;
		t1 = t1->wr_tsnext;
		count++;
	}
	fp.close();
		//fprintf(fp, "%-17lld%d proc %d 8 W 9 0 %8.8x%8.8x%8.8x%8.8x\n", t1->wr_ts, t1->wr_pid, t1->wr_lba, h0, h1, h2, h3);

	cout << "Lba count \t\t\t: " << count << endl;
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
"/media/chapram/6BC7-764E/localtrace/nptel-trace.txt"
*/
"/media/chapram/6BC7-764E/localtrace/imgvid-trace.txt"

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
	unsigned long long int rcount = 0, wcount = 0;
	int pid, op;
	unsigned int i, j; 
	unsigned long long reccount100000 = 1;
	unsigned long long int ts1, ts2, incr;
	//WriteReq *t1;
	int nb;

	ofstream fp("mail-read-trace.txt");
	

	//if (trace.openTrace(fnames, 7) != 0) {
	//if (trace.openTrace(fnames, 21) != 0) {
#ifndef	HOME
	trace = new IOTraceReader4K();
	//if (trace->openTrace(fnames, 21, 4096) != 0) {
	if (trace->openTrace(fnames, 1, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
	nb = 8;
#else
	trace = new IOTraceReaderHome();
	if (trace->openTrace(fnames, 21, 512) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
	nb = 1;
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

		if (s.seg_size > 1) 
			incr = (ts2 - ts1) / (s.seg_size - 1);
		else incr = 2;
		if (incr <= 0) incr = 2;

#ifndef	HOME
		s.seg_start /= 8;
#endif
		if (op == OP_READ) {
	
			for (i=0; i<s.seg_size; i++) {
				fp << dec << (ts1 + (i * incr)) << " " << pid << " proc " 
					<< ((s.seg_start + i) * nb)  << " " << nb << " R 3 0 ";
				for (j=0; j<16; j++) 
					fp << hex << setw(2) << setfill('0') << right << (unsigned int)s.seg_hash[i][j];
				fp << dec << endl;
			}
		}
		if (trace->getCurCount() >= (reccount100000 * 500000)) {
			cout << trace->getCurCount() << "\t number of records processed\n";
			reccount100000++;
		}
	}

	count = trace->getCurCount();			
	trace->closeTrace();	
	fp.close();

	cout << "\n" << count << " number of records, read \t= " << rcount << "  write \t= " << wcount << endl << endl;

	return 0;
}

