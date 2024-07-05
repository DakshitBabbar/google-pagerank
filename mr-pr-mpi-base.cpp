#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

using namespace MAPREDUCE_NS;
using namespace std;

void mymap(int, char *, int, KeyValue *, void *);
void sum(char *, int, char *, int, int *, KeyValue *, void *);
void update(char *, int, char *, int, void *);

struct data{
	float alpha = 0.85;
	vector<float> ranks;
	long num_pages;
};

//==============================================================================================//

int main(int narg, char **args)
{	
	vector<float> ranks;
	long num_pages;	
	
	struct data mydata;

  MPI_Init(&narg,&args);
	
  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;
  mr->timer = 0;

  MPI_Barrier(MPI_COMM_WORLD);
  
  
  string input_file;
  long num_mappers;
  long num_reducers;
	
	if(me == 0){
		string input_file = args[1];
		
		//read input from input_file to find total number of pages
		ifstream instream_pages(input_file);
		
		if(!instream_pages) {
		  cerr << "Error: file could not be opened" << endl;
		  exit(1);
		}
		
		//variable to store the maximaum pages number
		long page_max = 0;
		long x;
		while (!instream_pages.eof()) {  
			instream_pages >> x;
			if(x>page_max) page_max=x;
		}
		
		num_pages = page_max + 1;
		instream_pages.close();
		
		
		//read input from the input file
		ifstream instream(input_file);
		
		if(!instream) {
		  cerr << "Error: file could not be opened" << endl;
		  exit(1);
		}
		
		vector<vector<long>> adj_list(num_pages);
		
		long page;
		long outlink;
		while (!instream.eof()) {  
			instream >> page;
			instream >> outlink;
			
			adj_list[page].push_back(outlink);
		}
		adj_list[page].pop_back();
	 	instream.close();
	 	
	 	//output the adj_list in the adj_list file
	 	ofstream outstream("adj_list.txt");
	 	for(long i=0; i<num_pages; i++){
	 		outstream << i;
	 		for(auto e: adj_list[i]){
	 			outstream << " " << e;
	 		}
	 		outstream << endl;
	 	}
	 	outstream.close();
	}	
	//give specifications
	num_mappers = nprocs;
	num_reducers = nprocs;
  
  MPI_Barrier(MPI_COMM_WORLD);
  //double tstart = MPI_Wtime();

	MPI_Bcast(&num_pages, 1, MPI_LONG, 0, MPI_COMM_WORLD);	
	
	//initialize ranks
 	float rank_init = 1.0/num_pages;
 	vector<float> ranks_(num_pages, rank_init);
 	ranks = ranks_;
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	int num_iter = 1;
	for(int i=0; i<num_iter; i++){
		//cout << "iter " << i << endl; 
		//cout << "lol here" << endl;
		mydata.num_pages = num_pages;
		mydata.ranks = ranks;
		
		string fname ("adj_list.txt");
		char * cstr = new char [fname.length()+1];
  	strcpy(cstr, fname.c_str());
		
		mr->map((int) num_mappers, 1 , &cstr ,0, 0 , '\n', (int)(num_pages), mymap, &mydata);
		
		mr->collate(NULL);
		mr->reduce(sum, &mydata);

		MPI_Barrier(MPI_COMM_WORLD);
		//cout << "lol done 1" << ", " << me << endl;
		
		mr->gather(1);
		

		mr->scan(update, &mydata);
		ranks = mydata.ranks;
		
		MPI_Bcast(&ranks[0], num_pages, MPI_FLOAT, 0, MPI_COMM_WORLD);
		
		MPI_Barrier(MPI_COMM_WORLD);
	}
	
  //double tstop = MPI_Wtime();
	
  delete mr;
  
  if(me==0){
		float sum = 0.0;
		for (long i=0; i<num_pages; i++){
			sum += ranks[i];
		}
		 
		string output_file = args[2];
		ofstream outstream(output_file);
		for(long i=0; i<num_pages; i++){
			outstream << i << " =";
			outstream << " " << ranks[i] << endl;
		}
		outstream << "sum " << sum;
		outstream.close();
  }

  MPI_Finalize();
  
  
}

//==============================================================================================//
//mapping function
void mymap(int itask, char *data, int size, KeyValue *kv, void *ptr)
{
	struct data *mydata = (struct data*) ptr;
	float alpha = mydata->alpha;
	long num_pages = mydata->num_pages;
	vector<float> ranks = mydata->ranks;
		
  vector<pair<long, vector<long>>> adj_list;
	
	for(auto it=data; *it!='\0'; it++){
		
		string page_st = "";
		while(*it!=' ' && *it!='\n'){
			page_st += *it;
			it++;
		}
		
		long page = stoi(page_st);
		
		pair<long, vector<long>> mypair;
		mypair.first = page;
		
		vector<long> vec;
		while(*it!='\n'){
			it++;
			string outlink_st = "";
			while(*it!=' ' && *it!='\n'){
				outlink_st += *it;
				it++;
			}
			long outlink = stoi(outlink_st);
			
			vec.push_back(outlink);
		}
		
		mypair.second = vec;
		
		adj_list.push_back(mypair);
	}
	
	/*
	//following code to see the data for the indvidial processors
	
	cout << adj_list.size() << endl;
	cout << "proc " << itask << "'s data" << endl;
	for(auto e:adj_list){
		long p=e.first;
		vector<long> v=e.second;
		cout << p << ":";
		for(auto f:v){
			cout << " " << f;
		}
		cout << endl;
	}
	*/
	
	for(auto element: adj_list){
		long page = element.first;
		vector<long> outlinks = element.second;
		
		long num_outlinks = outlinks.size();
		//cout << "outlinks " << num_outlinks << " from " << page << endl;
		
		if(num_outlinks != 0){
			for(auto emit_page: outlinks){
				float contri = alpha*(ranks[page]/num_outlinks);
				//cout << contri << " for " << emit_page << " by " << page << " in mapper " << key << endl;
				//cout << "lol done done" << endl;
				
				kv->add((char *) &emit_page, sizeof(long), (char *) &contri,sizeof(float));
			}
		} else {
			
			for(long i=0; i<num_pages; i++){
				float contri = alpha*(ranks[page]/num_pages);
				//cout << contri << " for " << i << " by " << page << " in mapper " << key << endl;
				
				kv->add((char *) &i, sizeof(long), (char *) &contri,sizeof(float));
			}
		}
		
		
		for(long i=0; i<num_pages; i++){
			float contri = (1-alpha)*(ranks[page]/num_pages);
			//cout << contri << " for " << i << " by " << page << " in mapper " << key << endl;
			
			kv->add((char *) &i, sizeof(long), (char *) &contri,sizeof(float));
		}
	}
	//cout << "lol inside mapper" << ", " << itask << endl;
}

//==============================================================================================//
//reducing function
void sum(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{	
	float sum = 0.0;
	float *it = (float *) multivalue;
	for(int i=0; i<nvalues; i++){
		sum += *it;
		it++;
	}

  kv->add(key,keybytes,(char *) &sum, sizeof(float));
}

//==============================================================================================//
//update ranks fucntion
void update(char *key, int keybytes, char *value, int valuebytes, void *ptr)
{
	long *page_pt = (long *) key;
	long page = *page_pt;
	
	float *rank_pt = (float *) value;
	float rank = *rank_pt;
	
	//cout << page << ": " << rank << endl;
	
	((struct data *) ptr)->ranks[page] = rank;
}







