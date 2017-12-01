#include <cstdio>
#include <iostream>
#include <sstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>
#include <regex>
#include <cmath>
#include <list>
#include <fstream>
#include <algorithm>

std::string exec(const char* cmd) {
	std::array<char, 128> buffer;
	std::string result;
	std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
	if (!pipe) throw std::runtime_error("popen() failed!");
	while (!feof(pipe.get())) {
		if (fgets(buffer.data(), 128, pipe.get()) != nullptr)
			result += buffer.data();
	}
	return result;
}

std::string remove_white(std::string str) {
	std::string::iterator end_pos = std::remove(str.begin(), str.end(), ' ');
	str.erase(end_pos, str.end());
	return str;
}

int create() {
	std::string keyspace_result = exec("cqlsh -e \"CREATE KEYSPACE Neighbors WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};\" 172.17.0.2");
	std::string table_result = exec("cqlsh -e 'CREATE TABLE Neighbors.data ( node int, iteration int, value float, PRIMARY KEY (node, iteration) );' 172.17.0.2");
	/* NOT WORKING FOR SOME REASON - DON'T NEED ERROR HANDLING REALLY
	if ( keyspace_result != "" || table_result != "" ) {
		std::cout << "Issue creating keyspace / table!\n";
		return 1;
	}
	*/
	return 0;
}

int destroy() {
	std::string drop_result = exec("cqlsh -e 'drop keyspace Neighbors;' 172.17.0.2");
	/* NOT WORKING FOR SOME REASON - DON'T NEED ERROR HANDLING REALLY
	if ( drop_result != "" ) {
		std::cout << "Issue deleting keyspace!\n";
		return 1;
	}
	*/
	return 0;
}

float get(int node, int iter) {
	std::string pre1 = std::string("cqlsh -e 'SELECT JSON value FROM Neighbors.data WHERE node=");
	std::string pre2 = std::string(" AND iteration=");
	std::string pre3 = std::string(";' 172.17.0.2");

	std::stringstream node_stream, iter_stream;
	node_stream << node;
	iter_stream << iter;

	std::string select_result = exec( (pre1 + node_stream.str() + pre2 + iter_stream.str() + pre3).c_str() );

	size_t start = select_result.find("{\"value\": ");
	if (start == std::string::npos) {
		return NAN;
	}
	else {
		start += 10;
	}
	size_t end = select_result.find("}");

	std::string float_str = select_result.substr(start, end - start);

	return strtof( float_str.c_str(), 0 );
}

int put(int node, int iter, float val) {
	std::string pre1 ("cqlsh -e 'INSERT INTO Neighbors.data ( node, iteration, value ) VALUES ( ");
	std::string pre23 (", ");
	std::string pre4 (");' 172.17.0.2");

	std::stringstream node_strm, iter_strm, val_strm;
	node_strm << node;
	iter_strm << iter;
	val_strm << val;

	std::string insert_result = exec( (pre1 + node_strm.str() + pre23 + iter_strm.str() + pre23 + val_strm.str() + pre4).c_str() );
	return 0;
}

std::list<int> get_neighbors(int node, char * neigh_file) {
	std::list<int> neighbors;
	std::string line;

	std::ifstream neighbors_file (neigh_file);
	if (neighbors_file.is_open()) {
		while ( getline(neighbors_file,line) ) {

			line = remove_white(line);

			size_t node_start_pos = 0;
			size_t node_end_pos = line.find(":");
			std::string line_node_str = line.substr(node_start_pos, node_end_pos - node_start_pos);
			int line_node = atoi(line_node_str.c_str());

			if ( line_node == node ) {
				// erase input node from line, leaving just comma-delimited neighbor list
				line.erase(0, node_end_pos+1);

				size_t pos = 0;
				std::string neigh;
				while ((pos = line.find(",")) != std::string::npos) {
					neigh = line.substr(0, pos);
					neighbors.push_back( atoi(neigh.c_str()) );
					line.erase(0, pos + 1);
				}
				neighbors.push_back( atoi(line.c_str()) );
				break;
			}
		}
		neighbors_file.close();
	}
	return neighbors;
}

int main(int argc, char ** argv) {

	// get node number and initial value
	int node = atoi(argv[1]);
	float val = strtof( argv[2], 0 );
	std::cout << "starting process for node: " << node << ", with initial value: " << val << "\n";

	// initialize iteration counter and get max iterations
	int iter = 0;
	int iter_cap = atoi(argv[3]);

	// get name of neighbors file
	char * neigh_file = argv[4];

	// get the list of neighbors of node from neighbors file
	std::list<int> neighbors = get_neighbors(node, neigh_file);
	std::list<int>::iterator it;
	float size = neighbors.size() + 1.0;

	// put initial value for node, increase iterator
	put(node, iter, val); 
	iter++;

	while (iter < iter_cap) {
		for (it = neighbors.begin(); it != neighbors.end(); ++it) {
			float reqd_val = get(*it, iter-1);
			// NAN indicates value not yet in DB
			while ( std::isnan(reqd_val) ){
				reqd_val = get(*it, iter-1);
			}
			val += reqd_val;
		}
		val /= size;
		std::cout << val << "\n";
		put(node, iter, val);
		iter++;
	}

	return 0;
}
