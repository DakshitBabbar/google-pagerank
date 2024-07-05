#include <boost/config.hpp>
#if defined(BOOST_MSVC)
#   pragma warning(disable: 4127)

// turn off checked iterators to avoid performance hit
#   if !defined(__SGI_STL_PORT)  &&  !defined(_DEBUG)
#       define _SECURE_SCL 0
#       define _HAS_ITERATOR_DEBUGGING 0
#   endif
#endif

#include "mapreduce.hpp"
#include <iostream>
#include <vector>
#include <fstream>
using namespace std;

//===================================================================================//

float alpha = 0.85;
vector<float> ranks; 
long num_pages;

namespace pagerank {

//define datasource class
//this assigns data to each mapper, in form of a key value pair
template<typename MapTask>
class network_source: mapreduce::detail::noncopyable
{
	private:
    long       			sequence_;
    vector<vector<long>> adj_list_;
    long				step_;
    
    public:
    network_source(vector<vector<long>> adj_list, long step)
      : sequence_(0), adj_list_(adj_list), step_(step)
    {
    }

    bool const setup_key(typename MapTask::key_type &key)
    {
        key = sequence_++;
        return (key * step_ < num_pages);
        
    }

    bool const get_data(typename MapTask::key_type const &key, typename MapTask::value_type &value)
    {
        typename MapTask::value_type val;
        
        long str = key*step_;
        long end = min(str + step_, num_pages);
        for(long i=str; i<end; i++){
        	pair<long, vector<long>> element;
        	
        	element.first = i;
   			element.second = adj_list_[i];
   		
        	val.push_back(element);	
        }

        swap(val, value);
        return true;
    }
};

struct map_task : public mapreduce::map_task<long, vector<pair<long, vector<long>>> >
{
    template<typename Runtime>
    void operator()(Runtime &runtime, key_type const &key, value_type const &value) const
    {
    	for(auto element: value){
    		long page = element.first;
    		vector<long> outlinks = element.second;
    		
    		long num_outlinks = outlinks.size();
    		//cout << "outlinks " << num_outlinks << " from " << page << endl;
    		
    		if(num_outlinks != 0){
    			for(auto emit_page: outlinks){
					float contri = alpha*(ranks[page]/num_outlinks);
					//cout << contri << " for " << emit_page << " by " << page << " in mapper " << key << endl;
					//cout << "lol done done" << endl;
					runtime.emit_intermediate(emit_page, contri);
				}
    		} else {
    			
    			for(long i=0; i<num_pages; i++){
					float contri = alpha*(ranks[page]/num_pages);
					//cout << contri << " for " << i << " by " << page << " in mapper " << key << endl;
					
					runtime.emit_intermediate(i, contri);
				}
    		}
    		
    		
    		for(long i=0; i<num_pages; i++){
				float contri = (1-alpha)*(ranks[page]/num_pages);
				//cout << contri << " for " << i << " by " << page << " in mapper " << key << endl;
				
				runtime.emit_intermediate(i, contri);
			}
    	}
    }
};

struct reduce_task : public mapreduce::reduce_task<long, float>
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, key_type const &key, It it, It ite) const
    {
   		float sum = 0;
   		for(auto i=it; i!=ite; i++){
   			sum += *i;
   		}
   		
   		
   		ranks[key] = sum;
   		
   		runtime.emit(key, sum);
    }
};

typedef
mapreduce::job<pagerank::map_task,
               pagerank::reduce_task,
               mapreduce::null_combiner,
               pagerank::network_source<pagerank::map_task>
> job;

} // namespace pagerank

int main(int argc, char *argv[])
{
    mapreduce::specification spec;
    
    string input_file = argv[1];
    
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
    ifstream mystream;
    mystream.open(input_file);
    
    if(!mystream) {
      cerr << "Error: file could not be opened" << endl;
      exit(1);
    }
    
    vector<vector<long>> adj_list(num_pages);
    
    long page;
    long outlink;
    while (!mystream.eof()) {  
    	mystream >> page;
    	mystream >> outlink;
    	
    	
    	adj_list[page].push_back(outlink);
    }
    adj_list[page].pop_back();
   	mystream.close();
   	
   	//provide specifications
   	
   	if(argc>2)
   		spec.map_tasks = atoi(argv[2]);
   	
   	int reduce_tasks;
    if (argc > 3)
        reduce_tasks = atoi(argv[3]);
    else
        reduce_tasks = std::max(1U, std::thread::hardware_concurrency());
        
    spec.reduce_tasks = reduce_tasks;
    
    if(argc>4)
   		alpha = atof(argv[4]);
   	
   	//initialize ranks
   	float rank_init = 1.0/num_pages;
   	vector<float> ranks_(num_pages, rank_init);
   	ranks = ranks_;
	
	//start loop till for num_iter iterations
	/*for(auto e: adj_list){
		for(auto f: e){
			cout <<f << " ";
		}cout << endl;
	}*/
	//std::cout <<"Calculating Pageranks..." << endl;
	
	long num_iter = 50;
	for(long i=0; i<num_iter; i++){
		//cout << "\niter " << i << endl;
		
		pagerank::job::datasource_type datasource(adj_list, num_pages/reduce_tasks);
		
		pagerank::job job(datasource, spec);
		
		mapreduce::results result;
		
		job.run<mapreduce::schedule_policy::cpu_parallel<pagerank::job> >(result);
		//cout << "ranks:" << endl;	
		//for(auto e: ranks) cout << e << " "; cout << endl << endl;
	}
	
	float sum = 0.0;
  for (long i=0; i<num_pages; i++){
  	sum += ranks[i];
  }
   
	string output_file = argv[2];
  ofstream outstream(output_file);
 	for(long i=0; i<num_pages; i++){
 		outstream << i << " =";
 		outstream << " " << ranks[i] << endl;
 	}
 	outstream << "sum " << sum;
 	outstream.close();
  

	return 0;
}
