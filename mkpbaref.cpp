#include<iostream>
#include<fstream>
#include<string.h>
#include "pbaref.h"


char pbafile[] = "pbaref.dat";
struct PbaRef p;

int main() {
	int i;
	ofstream fs;
	
	fs.open(pbafile, ios::out | ios::binary | ios::trunc);
	if (fs.fail()) {
		cout << "pbaref.dat opening failure\n";
		exit(0);
	}
	fs.clear();
	memset(&p, 0, sizeof(struct PbaRef));	
	for (i=0; i<(1024*1024); i++)
		fs.write((char *)&p, sizeof(struct PbaRef));

	fs.close();
	return 0;
}
	
