/**
 * @author zhangdiao
 * File name management.
 */

#ifndef GRAPH_FILENAME_HPP_
#define GRAPH_FILENAME_HPP_

#include <string>
#include <sstream>

namespace graph{

	class GraphFileName {
	private:
		std::string fileName;

	public:
		GraphFileName():fileName("temp") {}

		GraphFileName(const std::string &_fileName):fileName(_fileName) {
		}

		inline void setBaseName(const std::string &baseName) {
			fileName = baseName;
		}

		inline std::string getFileName() {
			return fileName;
		}

		// .vertexdata: total vertices data file
		inline std::string graphVertexDataFileName() {
			std::stringstream temp;
			temp << fileName << ".vertexdata";
			return temp.str();
		}

		// .vertexdata_subgraph100_200: vertices data of subgraph
		inline std::string subgraphVertexDataFileName(int st, int en) {
			std::stringstream temp;
			temp << fileName << ".vertexdata_subgraph" << st << "_" << en;
			return temp.str();
		}

		// .inedge100_200: subgraph edge list
		inline std::string subgraphInEdgeFileName(int st, int en) {
			std::stringstream temp;
			temp << fileName << ".inedge" << st << "_" << en;
			return temp.str();
		}

		// .inedgedata100_200: subgraph edge data 
		inline std::string subgraphInEdgeDataFileName(int st, int en) {
			std::stringstream temp;
			temp << fileName << ".inedgedata" << st << "_" << en;
			return temp.str();
		}

		// .indegree_prefixsum: the prefix sum of in degree. 
		// the data is used to calculate the in degree of each vertex
		inline std::string graphInDegreePrefixSumFileName() {
			std::stringstream temp;
			temp << fileName << ".indegree_prefixsum";
			return temp.str();
		}

		// subgraph prefix sum for in degree
		inline std::string subgraphInDegreePrefixSumFileName(int st, int en) {
			std::stringstream temp;
			temp << fileName << ".indegree_prefixsun_subgraph" << st << "_" << en;
			return temp.str();
		}

		// .indegree: the file of in degree for totoal vertices
		inline std::string graphInDegreeFileName() {
			std::stringstream temp;
			temp << fileName << ".indegree";
			return temp.str();
		}

		// the file of in degree for subgraph vertices
		inline std::string subgraphInDegreeFileName(int st, int en) {
			std::stringstream temp;
			temp << fileName << ".indegree_subgraph" << st << "_" << en;
			return temp.str();
		}

		inline std::string graphOutDegreeFileName() {
			std::stringstream temp;
			temp << fileName << ".outdegree";
			return temp.str();
		}

		inline std::string subgraphOutDegreeFileName(int st, int en) {
			std::stringstream temp;
			temp << fileName << ".outdegree_subgraph" << st << "_" << en;
			return temp.str();
		}

		// graph intervals file
		inline std::string graphSubInterval() {
			std::stringstream temp;
			temp << fileName << ".subgraph_interval";
			return temp.str();
		}

		// the info: the number of subgraph, edge number and so on. 
		inline std::string memoryAllocInfoName() {
			std::stringstream temp;
			temp << fileName << ".memory_alloc";
			return temp.str();
		}

		// temp file for binary graph
		inline std::string binaryTempFileName(int index) {
			std::stringstream temp;
			temp << fileName << ".b_temp" << index;
			return temp.str();
		}

		inline std::string sortedFileName() {
			std::stringstream temp;
			temp << fileName << ".sorted_file";
			return temp.str();
		}
		
		inline std::string tempFileName(const char *postfix){
			std::stringstream temp;
			temp << fileName << postfix;
			return temp.str();
		}
	};
}

#endif /* GRAPH_FILENAME_HPP_ */
