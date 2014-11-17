/**
 * @author zhangdiao
 * @brief 
 * Alloc memory for graph computation.
 */

#ifndef MEMORY_ALLOC_HPP_
#define MEMORY_ALLOC_HPP_

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "../util/graph_filename.hpp"
#include "../util/common.hpp"

typedef unsigned long long num_t;


namespace graph{
	struct MemoryAllocResult{
		size_t forEdgeAndEdgeData;
		size_t forVertexData;
		size_t forOutDegreeData;
		bool totalVertexDataInMemory;
		bool totalDegreeDataInMemory;
		bool totalInDegreeDataInMemory;
		bool totalInDegreePrefixSumInMemory;
		int subgraphNumber;
		size_t cacheForEdge;
		size_t cacheForEdgeData;
		int vertexNumber;
		int edgeNumber;
		int edgeNumberPerSubgraph;
		MemoryAllocResult(){}
		MemoryAllocResult(size_t _forEE, size_t _forV,size_t _forD,size_t _cacheForEdge,size_t _cacheForEdgeData,int subgraphNumber,bool _vertexDataInMemory=false,bool _degreeDataInMemory=false,bool _inDegreeDataInMemory = false,bool _totalInDegreePrefixSumInMemory = false):forEdgeAndEdgeData(_forEE),
				forVertexData(_forV),forOutDegreeData(_forD),totalVertexDataInMemory(_vertexDataInMemory),totalDegreeDataInMemory(_degreeDataInMemory),totalInDegreeDataInMemory(_inDegreeDataInMemory),totalInDegreePrefixSumInMemory(_totalInDegreePrefixSumInMemory),subgraphNumber(subgraphNumber),
				cacheForEdge(_cacheForEdge),cacheForEdgeData(_cacheForEdgeData){}
	};

	template <typename EdgeDataType>
	struct EdgeWithValue{
		int src;
		int dst;
		EdgeDataType edgeValue;
		EdgeWithValue<EdgeDataType>(){}
		EdgeWithValue(int _src,int _dst,EdgeDataType _edgeValue):src(_src),dst(_dst),edgeValue(_edgeValue){}
		bool operator< (EdgeWithValue<EdgeDataType> &other){
			return dst < other.dst;
		}
		bool operator<= (EdgeWithValue<EdgeDataType> &other){
			return dst < other.dst;
		}
		bool operator== (EdgeWithValue<EdgeDataType> &other){
			return dst == other.dst;
		}
	};

	template <typename EdgeDataType>
	EdgeWithValue<EdgeDataType> parseLine(char *line){
		int src0 = 0,dst0 = 0,edgevalue0 =0;
		removeN(line);
		char splitChars[] = "\t, ";
		char * t;
		t = strtok(line,splitChars);
		if(t != NULL){
			src0 = atoi(t);
		}
		t = strtok(NULL,splitChars);
		if(t != NULL){
			dst0 = atoi(t);
		}
		t = strtok(NULL,splitChars);
		if(t != NULL){
			edgevalue0 = atoi(t);
		}

		return EdgeWithValue<EdgeDataType>(src0,dst0,edgevalue0);
	}
	int max3(int max,int src,int dst){
		if(src > max){
			max = src;
		}
		if(dst > max){
			max  = dst;
		}
		return max;
	}
	template <typename VertexDataType, typename EdgeDataType>
	void memoryAlloc(MemoryAllocResult &mar,int totalMemory,std::string &fileName,int subgraphNumber,bool bothDegreeUsed = true){

		// default max number per subgraph. It may be modified later.
		mar.edgeNumberPerSubgraph = MAX_EDGE_NUMBER_PER_SUB;
		mar.subgraphNumber = subgraphNumber; // the appropriate number of subgraph
		//TODO: Memory Allocation
		/*
		// totalMemory:Mb
		size_t leftMemory = totalMemory * 1024 * 1024;
		FILE *inf = fopen(fileName.c_str(),"r");
		if(inf == NULL){
			std::cout << "Error: can not open the graph file." << std::endl;
			assert(false);
		}
		char line[1024];
		num_t lineNumber = 0;
		int maxVertexId = 0;
		while(fgets(line,1024,inf) != NULL){
			if(line[0] == '#' || line[0] == '%'){
				continue;
			}

			EdgeWithValue<EdgeDataType> temp = parseLine<EdgeDataType>(line);
			// assume all the graph file start from 1 or 0.
			if(temp.src > 1 && temp.dst >1){
				if(temp.edgeValue != 0){
					maxVertexId = temp.src + temp.dst;
					lineNumber = temp.edgeValue;
				}
				else{
					maxVertexId = temp.src;
					lineNumber = temp.dst;
				}
			}
			break;
		}

		if(maxVertexId == 0){
			while(fgets(line,1024,inf) != NULL){
				EdgeWithValue<EdgeDataType> temp = parseLine<EdgeDataType>(line);
				maxVertexId = max3(maxVertexId,temp.src,temp.dst);
				++ lineNumber;
			}
		}
		fclose(inf);
		*/
		// Just for experiments.
		size_t forEE = 300*1024*1024;// memory for read Edge and Edge Data
		size_t forV = 400 * 1024 * 1024; // ...Vertex data
		size_t forD = 400 * 1024 * 1024; // ...Degree data
		size_t cacheForEdge = 100*1024*1024;// Cache Memory for Edge
		size_t cacheForEdgeData = 400*1024*1024;// Cache memory for edge data

		mar.cacheForEdge = cacheForEdge;
		mar.cacheForEdgeData = cacheForEdgeData;
		mar.forEdgeAndEdgeData = forEE;
		mar.forVertexData = forV;
		mar.forOutDegreeData = forD;
		mar.totalVertexDataInMemory = true;
		mar.totalDegreeDataInMemory = true;
		mar.totalInDegreeDataInMemory = true;
		mar.totalInDegreePrefixSumInMemory = true;
	}
}

#endif /* MEMORY_ALLOC_HPP_ */
