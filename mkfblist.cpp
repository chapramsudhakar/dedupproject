#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>

using namespace std;

#include "fblist.h"

string fname = "fblist.dat";
fstream fbl;

int main() {
	int i;
	struct FBEntry p;
	//unsigned int nb, pba, next, first, last;

	fbl.open(fname.c_str(), ios::out | ios::binary);
	memset(&p, 0, sizeof(struct FBEntry));

	// Write first entry
	p.fbe_pba = 0xffffffff;		// Some non-zero pba
	p.fbe_snode = 0;
	p.fbe_nb = (1024 * 1024);	// Last entry number
	p.fbe_next = 2;			// First free entry number
	fbl.write((char *)&p, sizeof(struct FBEntry));

	// Write next n-2 entries as a
	// linked list of free entries
	p.fbe_pba = 0;
	p.fbe_nb = 0;
	for (i=2; i<(1024*1024); i++) {
		p.fbe_next = i + 1;
		fbl.write((char *)&p, sizeof(struct FBEntry));
	}

	// Write the last entry
	p.fbe_next = 0;
	fbl.write((char *)&p, sizeof(struct FBEntry));
	fbl.close();
}

