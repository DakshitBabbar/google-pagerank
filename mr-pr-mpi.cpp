#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <fstream>
#include <iostream>

#include <string.h>
#include <sstream>
#include <vector>

#include <mpi.h> 

using namespace std;

/*
function to make the usable input file from the input file=========================================================================
*/
void MR_makeAdjList(int me, string input_file, long *num_pages, MPI_Comm comm){

	if(me == 0){
		//read input from input_file to find total number of pages
		ifstream instream(input_file);
		
		if(!instream) {
		  cerr << "Error: file could not be opened" << endl;
		  exit(1);
		}
		
		//variable to store the maximaum pages number
		long page_max = 0;
		long x;
		while (!instream.eof()) {  
			instream >> x;
			if(x>page_max) page_max=x;
		}
		
		*num_pages = page_max + 1;
		instream.close();
		
		
		//read input from input_file to make adj_list
		ifstream instream_new(input_file);
		
		if(!instream_new) {
		  cerr << "Error: file could not be opened" << endl;
		  exit(1);
		}
		
		vector<vector<long>> adj_list(page_max + 1);
		
		long page;
		long outlink;
		while (!instream_new.eof()) {  
			instream_new >> page;
			instream_new >> outlink;
			
			adj_list[page].push_back(outlink);
		}
		adj_list[page].pop_back();
		
	 	instream_new.close();


	 	//output the adj_list in the adj_list file
	 	ofstream outstream("auxilliary.txt");
	 	for(long i=0; i<*num_pages; i++){
	 		outstream << i;
	 		for(auto e: adj_list[i]){
	 			outstream << " " << e;
	 		}
	 		outstream << endl;
	 	}
	 	outstream.close();
	}	
	
	MPI_Barrier(comm);
}

/*
function to distribute data among different mappers================================================================================
*/
void MR_datasource(int me, long nprocs, long num_pages, string filename, char* recv, MPI_Comm comm){
	//form arrays for scounts and disps
	vector<long> scounts(nprocs), disps(nprocs);
	
	//pointer to be filled with input file string
	char* cstr;
	
	if(me==0){
		//adj_list flattened into string
		string adj_str;
		
		//read file and fill the above string
		ifstream instream(filename);
			
		if(!instream) {
			cerr << "Error: file could not be opened" << endl;
			exit(1);
		}

	 	//fill up adj_str with data
		ostringstream ss;
		ss << instream.rdbuf(); 
		adj_str = ss.str();
		
		instream.close();
		
		//convert adj_str to array of chars in cstr
		cstr = new char [adj_str.length()+1];
		strcpy(cstr, adj_str.c_str());
		
		//form arrays for scounts and disps
		long chunk_sz = num_pages/nprocs;
		long rem = num_pages - chunk_sz*nprocs;
		
		char* ptr = cstr;
		long disp = 0; 
		long i=0;
		
		while(*ptr != '\0'){
			//cout << i  << ":" << endl;
			disps[i] = disp;
			
			long chunk;
			if(rem>0){
				chunk = chunk_sz + 1;
				rem--;
			} else {
				chunk = chunk_sz;
			}
			
			long scount = 1;
			for(int j=0; j<chunk; j++){
				while(*ptr != '\n'){
					//cout << *ptr;
					ptr++;
					scount++;
					disp++;
				}		
				//cout << *ptr;
				ptr++;
				scount++;
				disp++;	
			}
			//cout << endl;

			scounts[i] = scount-1;
			i++;
		}		
		
		scounts[nprocs-1]++;
	}
	
	MPI_Barrier(comm);
	
	//provide every proc with scounts and disps
	MPI_Bcast(&scounts[0], nprocs, MPI_LONG, 0, comm);
	MPI_Bcast(&disps[0], nprocs, MPI_LONG, 0, comm);
	
	
	MPI_Barrier(comm);
	for(auto e:disps) cout << e << " "; cout << endl;
	
	//scatter the array of chars to every proc
	recv = new char [scounts[me]];
	MPI_Scatterv(cstr, (const int*)&scounts[0], (const int*)&disps[0], MPI_CHAR, recv, (int)scounts[me], MPI_CHAR, 0, comm);

	MPI_Barrier(comm);
	
	recv[scounts[me]-1] = '\0';
	
	MPI_Barrier(comm);
}

/*
mapping function ==================================================================================================================
*/
void MR_map(int me, char *data, long num_pages, vector<float> ranks, float alpha, char *map_emit, long* my_emit_size, MPI_Comm comm)
{	
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
	
	string emit_string = "";
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
				
				ostringstream key, value;
				key << emit_page;
				value << contri;

				string key_str = key.str();
				string value_str = value.str();

				emit_string += key_str + " " + value_str + "\n";
			}
		} else {

			for(long i=0; i<num_pages; i++){
				float contri = alpha*(ranks[page]/num_pages);
				//cout << contri << " for " << i << " by " << page << " in mapper " << key << endl;

				ostringstream key, value;

				key << i;
				value << contri;

				string key_str = key.str();
				string value_str = value.str();

				emit_string += key_str + " " + value_str + "\n";
			}
		}


		for(long i=0; i<num_pages; i++){
			float contri = (1-alpha)*(ranks[page]/num_pages);
			//cout << contri << " for " << i << " by " << page << " in mapper " << key << endl;

			ostringstream key, value;

			key << i;
			value << contri;

			string key_str = key.str();
			string value_str = value.str();

			emit_string += key_str + " " + value_str + "\n";
		}
	}
	
	//return th size of emit_string + '\0'
	long emit_size = emit_string.length() + 1;
	
	//convert emit_string to char array map_emit
	map_emit = new char [emit_size];
	strcpy(map_emit, emit_string.c_str());
	map_emit[emit_size-1] = '\n'; //this will make all the emitted strings have a '\n' at the end instead of '\0'
	
	*my_emit_size = emit_size;
	MPI_Barrier(comm);
}



/*
collate function to collect all the key value emits from all mappers and redistribute accordingly to all reducers.=================
*/
void MR_collate(int me, long num_pages, long map_emit_size, char* map_emit, MPI_Comm comm){
	//gather all the keys value pairs in proc 0
	vector<long> rcounts(num_pages), disps(num_pages);
	MPI_Gather(&map_emit_size, 1, MPI_LONG, &rcounts[0], 1, MPI_LONG, 0, comm);
	
	long all_map_emits_size;
	
	if(me==0){
		disps[0] = 0;
		all_map_emits_size = rcounts[0];
		for(long i=1; i<num_pages; i++){
			disps[i] = disps[i-1] + rcounts[i-1] + 1;
			all_map_emits_size += rcounts[i];
		}
	}
	
	char* all_map_emits;
	if(me==0){
		all_map_emits = new char [all_map_emits_size + 1];
	}
	
	MPI_Gatherv((void*) map_emit, map_emit_size, MPI_CHAR, (void*)all_map_emits, (const int*)&rcounts[0], (const int*)&disps[0], MPI_CHAR, 0, comm);
	
	if(me==0){
		all_map_emits[all_map_emits_size - 1] = '\0';
		
		//make key-multi-value vector in proc 0
		vector<vector<float>> key_multi_value_vec(num_pages);
	
		for(auto it=all_map_emits; *it!='\0'; it++){
			
			string page_st = "";
			while(*it!=' ' && *it!='\n'){
				page_st += *it;
				it++;
			}
			
			long page = stoi(page_st);
			
			while(*it!='\n'){
				it++;
				string outlink_st = "";
				while(*it!=' ' && *it!='\n'){
					outlink_st += *it;
					it++;
				}
				float outlink = stof(outlink_st);
				
				key_multi_value_vec[page].push_back(outlink);
			}
		}
		
		//output the adj_list in the adj_list file
	 	ofstream outstream("auxilliary.txt");
	 	for(long i=0; i<num_pages; i++){
	 		outstream << i;
	 		for(auto e: key_multi_value_vec[i]){
	 			outstream << " " << e;
	 		}
	 		outstream << endl;
	 	}
	 	outstream.close();
	}
	
	MPI_Barrier(comm);
}

/*
reducing function==================================================================================================================
*/
void MR_reduce(int me, char *data, long num_pages, char *reduce_emit, long* my_emit_size, MPI_Comm comm)
{	
  vector<pair<long, vector<float>>> adj_list;
	
	for(auto it=data; *it!='\0'; it++){
		
		string page_st = "";
		while(*it!=' ' && *it!='\n'){
			page_st += *it;
			it++;
		}
		
		long page = stoi(page_st);
		
		pair<long, vector<float>> mypair;
		mypair.first = page;
		
		vector<float> vec;
		while(*it!='\n'){
			it++;
			string outlink_st = "";
			while(*it!=' ' && *it!='\n'){
				outlink_st += *it;
				it++;
			}
			float outlink = stof(outlink_st);
			
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
	
	string emit_string = "";
	for(auto element: adj_list){
		long page = element.first;
		vector<float> outlinks = element.second;
		
		float sum = 0.0;
		for(auto e: outlinks){
			sum+=e;
		}
		
		ostringstream key, value;

		key << page;
		value << sum;
		
		string key_str = key.str();
		string value_str = value.str();
		
		emit_string += key_str + " " + value_str + "\n";
	}
	
	//size of emit_string + '\0'
	long emit_size = emit_string.length() + 1;
	
	//convert emit_string to char array map_emit
	reduce_emit = new char [emit_size];
	strcpy(reduce_emit, emit_string.c_str());
	reduce_emit[emit_size-1] = '\n'; //this will make all the emitted strings have a '\n' at the end instead of '\0'
	
	*my_emit_size = emit_size;
	MPI_Barrier(comm);
}

/*
gather final key values and update the ranks vector (only proc 0 updates the rank vector)==========================================
*/
void MR_gather(int me, long num_pages, float* ranks, long reduce_emit_size, char* reduce_emit, MPI_Comm comm){
	//gather all the keys value pairs in proc 0
	vector<long> rcounts(num_pages), disps(num_pages);
	MPI_Gather(&reduce_emit_size, 1, MPI_LONG, &rcounts[0], 1, MPI_LONG, 0, comm);
	
	long all_reduce_emits_size = 0;
	
	if(me==0){
		disps[0] = 0;
		all_reduce_emits_size = rcounts[0];
		for(long i=1; i<num_pages; i++){
			disps[i] = disps[i-1] + rcounts[i-1] + 1;
			all_reduce_emits_size += rcounts[i];
		}
	}
	
	char* all_reduce_emits;
	if(me==0){
		all_reduce_emits = new char [all_reduce_emits_size + 1];
	}
	
	MPI_Gatherv((void*)reduce_emit, reduce_emit_size, MPI_CHAR, (void*)all_reduce_emits, (const int*)&rcounts[0], (const int*)&disps[0], MPI_CHAR, 0, comm);
	
	if(me==0){
		all_reduce_emits[all_reduce_emits_size - 1] = '\0';
		
		//make key-multi-value vector in proc 0
		vector<float> final_vec(num_pages);
	
		for(auto it=all_reduce_emits; *it!='\0'; it++){
			
			string page_st = "";
			while(*it!=' ' && *it!='\n'){
				page_st += *it;
				it++;
			}
			
			long page = stoi(page_st);
			
			while(*it!='\n'){
				it++;
				string outlink_st = "";
				while(*it!=' ' && *it!='\n'){
					outlink_st += *it;
					it++;
				}
				float outlink = stof(outlink_st);
				
				final_vec[page] = outlink;
			}
		}
		
		for(int i=0; i<num_pages; i++){
			ranks[i] = final_vec[i];
		}
	}
	
	MPI_Barrier(comm);
}


//main function====================================================================================================================

int main(int narg, char **args){		

	//initializations
  MPI_Init(&narg,&args);
	
  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);


  MPI_Barrier(MPI_COMM_WORLD);
  
  
  //set local variables
  float alpha = 0.85;
  vector<float> ranks;
	long num_pages;	
  
  
  //get input file name from terminal and form an adjescency list
  string input_file = args[1];
  

  MR_makeAdjList(me, input_file, &num_pages, MPI_COMM_WORLD);
  
  
  //broadcast num_pages to all the procs
  MPI_Bcast(&num_pages, 1, MPI_LONG, 0, MPI_COMM_WORLD);	
	
  
  MPI_Barrier(MPI_COMM_WORLD);
  
  
  //provide specifications
  long num_mappers, num_reducers;
  if(narg>2){
  	num_mappers = atoi(args[2]);
		num_reducers = atoi(args[3]);
  } else {
  	num_mappers = nprocs;
  	num_reducers = nprocs;
  }
	
  MPI_Barrier(MPI_COMM_WORLD);


	//broadcast total number of pages to all the procs
	MPI_Bcast(&num_pages, 1, MPI_LONG, 0, MPI_COMM_WORLD);	
	
	
	//initialize ranks
 	float rank_init = 1.0/num_pages;
 	ranks.resize(num_pages, rank_init);
 	
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	//calculate page ranks
	int num_iter = 5;
	for(int i=0; i<num_iter; i++){
		//provide data to all mapping procs
		char* mapper_data;		
		MR_datasource(me, nprocs, num_pages, "auxilliary.txt", mapper_data, MPI_COMM_WORLD);
		
		cout << "lol done"<< me << endl;
		
		//run the mapping function
		long map_emit_size;
		char* map_emit;
		MR_map(me, mapper_data, num_pages, ranks, alpha, map_emit, &map_emit_size, MPI_COMM_WORLD);
		
		//collate the emitted value pairs
		MR_collate(me, num_pages, map_emit_size, map_emit, MPI_COMM_WORLD);
		
		//provide data to all reducing procs
		char* reducer_data;
		MR_datasource(me, nprocs, num_pages, "auxilliary.txt", reducer_data, MPI_COMM_WORLD);
		
		//run reducing function
		long reduce_emit_size;
		char* reduce_emit;
		MR_reduce(me, reducer_data, num_pages, reduce_emit, &reduce_emit_size, MPI_COMM_WORLD);
		
		//gather final key values
		MR_gather(me, num_pages, &ranks[0], reduce_emit_size, reduce_emit, MPI_COMM_WORLD);

		MPI_Barrier(MPI_COMM_WORLD);
		
		//broadcast the updated ranks to all the procs
		MPI_Bcast(&ranks[0], num_pages, MPI_FLOAT, 0, MPI_COMM_WORLD);
		
		MPI_Barrier(MPI_COMM_WORLD);
	}

  MPI_Finalize();
  
  if(me==0){
			for(long i=0; i<num_pages; i++){
			cout << i << ": " << ranks[i] << endl;  
		}
  }
  
  return 0;
}




































