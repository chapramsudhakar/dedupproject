#include <iostream>
#include <iomanip>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <sys/time.h>
//#include "dedupconfig.h"
//#include "dedupn.h"
#include "iotracereader.h"


char fnames[16][100] = {
"/media/chapram/6BC7-764E/localtrace/books-trace.txt",
"/media/chapram/6BC7-764E/localtrace/slides-trace.txt",
"/media/chapram/6BC7-764E/localtrace/imgvid-trace.txt",
"/media/chapram/6BC7-764E/localtrace/nptel-trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.0.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.1.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.2.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.3.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.4.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.5.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.6.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.7.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.8.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.9.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.10.trace.txt",
"/media/chapram/6BC7-764E/localtrace/linux-5.11.trace.txt"
};
unsigned int segs[2049];
unsigned int seglen[2049];
unsigned long long offset[20];
unsigned int count=0;

int main(int argc, char** argv) {
		
	IOTraceReader *trace;
	struct segment4k s;
	int rcount = 0, wcount = 0;
	int pid, op;
	unsigned int i, j; 
	unsigned long long reccount100000 = 1;
	unsigned long long int ts1, ts2;
	int nb;
	unsigned long long int nanot;
	struct timeval tv;
	double p;
	int outputmode = 0;

	ofstream fp("linux-read-trace.txt");
	

	//if (trace.openTrace(fnames, 7) != 0) {
	//if (trace.openTrace(fnames, 21) != 0) {
	trace = new IOTraceReader4K();
	if (trace->openTrace(&fnames[4], 12, 4096) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}
	nb = 8;

	gettimeofday(&tv, NULL);
	nanot = tv.tv_sec * 1000000000 + tv.tv_usec * 1000;
	nanot %= 100000000000000;
	outputmode = 0;
	rcount = 0;
	while (trace->readNextSegment(&pid, &op, &ts1, &ts2, &s)) {
		if (outputmode == 1) {
			for (i=0; i<s.seg_size; i++) {
				fp << dec << nanot << " " << pid << " proc " 
					<< ((s.seg_start + i) * nb)  << " " << nb << " R 7 2 ";
				for (j=0; j<16; j++) 
					fp << hex << setw(2) << setfill('0') << right << (unsigned int)s.seg_hash[i][j];
				fp << dec << endl;
				nanot += (((random()%2)*1000000) + (random() % 1000000));
			}
			rcount -= s.seg_size;
			if (rcount <= 0) {
				outputmode = 0;
				rcount = 0;
			}
		}
		else {
			// Should the offset be remembered?
			p = drand48();
			if (p < 0.005) {
				if (count >= 20) {
					for (i=1; i<20; i++)
						offset[i-1] = offset[i];
					count = 19;
				}
				offset[count++] = trace->ifs.tellg();
			}
			
			// Should output be started
			p = drand48();

			if (p < 0.03) {
				outputmode = 1;
				rcount = random() % 100;
				if (count > 0) {
					i = random() % count;
					trace->ifs.seekg(offset[i]);
					i++;
					while (i < count) {
						offset[i-1] = offset[i];
						i++;
					}
					count--;
				}
			}
			nanot += (random() % 100000);
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

