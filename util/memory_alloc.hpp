/**
 * @author zhangdiao
 * Alloc memory for graph computation.
 */

#ifndef MEMORY_ALLOC_HPP_
#define MEMORY_ALLOC_HPP_

#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "graph_filename.hpp"
#include "common.hpp"
#include "parse_config.hpp"
#include "../core/graph_type.hpp"

typedef unsigned long long num_t;


namespace graph{

	const int  DEFAULT_SUBGRAPH_NUMBER = 1; 
	const size_t MAXIMUM_EDGE_NUMBER_PER_SUBGRAPH  = 40000000;
	const int AVERAGE_SCALE_OF_EDGE_VS_VERTICES = 5;
	
	struct MemoryAllocResult {

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

		MemoryAllocResult() {}
		MemoryAllocResult(size_t _forEE, size_t _forV, size_t _forD, size_t _cacheForEdge, size_t _cacheForEdgeData,
			int subgraphNumber, bool _vertexDataInMemory=false, bool _degreeDataInMemory=false, bool _inDegreeDataInMemory = false,
			bool _totalInDegreePrefixSumInMemory = false):forEdgeAndEdgeData(_forEE), forVertexData(_forV), forOutDegreeData(_forD),
			totalVertexDataInMemory(_vertexDataInMemory), totalDegreeDataInMemory(_degreeDataInMemory), totalInDegreeDataInMemory(_inDegreeDataInMemory),
			totalInDegreePrefixSumInMemory(_totalInDegreePrefixSumInMemory), subgraphNumber(subgraphNumber),
			cacheForEdge(_cacheForEdge),cacheForEdgeData(_cacheForEdgeData) {}
	};

	template <typename EdgeDataType>
	EdgeWithValue<EdgeDataType> parseLine(char *line) {

		int src0 = 0, dst0 = 0, edgevalue0 =0;
		removeN(line);
		char splitChars[] = "\t, ";
		char * t;
		t = strtok(line, splitChars);
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

	/*
		1,calculate the best-fit subgraph number by using 
		totalMemory, subgraphNumber, #edge and #vertices.
		2,set the memory allocation info.
	*/
	template <typename VertexDataType, typename EdgeDataType>
	void memoryAlloc(MemoryAllocResult &mar, std::string &fileName, const ParseCmd &pCmd, GraphOrder go = SOURCE_ORDER, bool bothDegreeUsed = true) {
		
		int nEdgeByUser = pCmd.getConfigInt("nedges");
		int nVerticesByUser = pCmd.getConfigInt("nvertices");
		
		size_t nEdge = 0;
		size_t nVertices = 0;
		if (nEdgeByUser == 0 || nVerticesByUser == 0) {
			// count the number of edge and vertices
			std::ifstream inf (fileName.c_str());
			if (!inf.good()) {
				std::cout << "Error: can not open the graph file!" << std::endl;
				assert(false);
			}

			vertex_t preId = -1;
			vertex_t startId = 0;
			char line[1024] = {0};
			EdgeWithValue<EdgeDataType> ewv;
			int targetId = 0;

			while (inf.getline(line, 1024)) {

				if (line[0] == '#' || line[0] == '%'){
					continue;
				}
				++ nEdge;

				ewv = parseGraphLine<EdgeDataType>(line);
				if (go == SOURCE_ORDER) {
					targetId = ewv.src;
				} else if (go == DESTINATION_ORDER) {
					targetId = ewv.dst;
				}
				
				if (preId != -1 && targetId != preId) {
					while (startId < preId) {
						++ nVertices;
						++ startId;
					}
					++ nVertices;
					startId = preId + 1;
				}

				preId = targetId;
			}

			while (startId < preId) {
				++ nVertices;
				++ startId;
			}
			++ nVertices;
		} else {
			nVertices = (size_t)nVerticesByUser;
			nEdge = (size_t)nEdgeByUser;
		}

		// test
		nVertices = 64000000;
		nEdge = 1500000000;


		// alloc memory according to #edge and #vertices
		// the unit of memory is mega
		int totalMemoryInMega = pCmd.getConfigInt("totalMemory");
		size_t totalMemory =  totalMemoryInMega * 1024L * 1024L;

		if (totalMemory <= 0) {
			std::cout << "Error: have no total memory information in configuration file.";
			assert(false);
		}

		/*
			minimum system requirement of main memory:
			1,Ubuntn Server: 192M
			2,Centos Server: 256M

			Assume the minimum total memory is 2048M and the available memory percentage is loaded from configuration file. 
		*/
		float availMemoryPercentage = 0.0F;
		if (totalMemoryInMega >= 2048 && totalMemoryInMega <= 3072) {

			availMemoryPercentage = 0.7F;
		} else if (totalMemoryInMega > 3072 && totalMemoryInMega <= 4096) {

			availMemoryPercentage = 0.8F;
		} else if (totalMemoryInMega > 4096) {

			availMemoryPercentage = 0.95F;
		} else {
			std::cout << "Error: The total memory in Mega is less than 2048." << std::endl;
			assert(false);
		}

		size_t totalAvailableMemory = 0.0F;
		if (availMemoryPercentage < 0.9F) {
			totalAvailableMemory = (size_t)(availMemoryPercentage * totalMemory);
		} else {
			totalAvailableMemory = totalMemory - 1000 * 1024L * 1024L;
		}
		 
		/*
			计算过程中需要的数据文件包括：1，顶点值文件；2，顶点入度值文件；3，顶点入度前缀和文件；4，入边文件；5，入边权值文件；6，出度值文件(非必需)
			内存分配：1，计算所有顶点值是否可以加载进内存，且保证剩余内存可足够提供边其他结构。对于GraphChi中使用的数据集，边数与顶点个数的比为15-35倍左右。
			若顶点可全部加载进内存，则需要保证预留给边和边值的可用内存百分子大于配置项：memoryPercentageForEdgeAndEdgeData(可调整该值，对比效果)。
			[1]若不满足配置比例，则设置totalVertexDataInMemory = false; 同时顶点的度、入度前缀和等文件也不能全部加载进内存。
			[2]若满足配置比例，则按照优先级，依次判断入度、入度前缀和是否可以全部加载进内存。

			同时，考虑到I/O的并行性，划分子图个数最好大于2。
		*/
		size_t minimalMemoryForEdgeAndData = (size_t)(pCmd.getConfigInt("memoryPercentageForEdgeAndEdgeData") / 100.0F * totalAvailableMemory);

		int subgraphNumber = pCmd.getConfigInt("nsub");
		size_t averageEdgeNumberPerSubgraph = 0;
		if (subgraphNumber != 0) {

			averageEdgeNumberPerSubgraph = nEdge / subgraphNumber + 1;

		} else {

			subgraphNumber = DEFAULT_SUBGRAPH_NUMBER;
			averageEdgeNumberPerSubgraph = nEdge / subgraphNumber + 1;
		}

		size_t memoryForCalculateNSub = minimalMemoryForEdgeAndData * 0.8F;
		while (averageEdgeNumberPerSubgraph * (sizeof(GraphInEdge) + sizeof(GraphEdgeData<EdgeDataType>) + sizeof(int))
			>  memoryForCalculateNSub) {

			++ subgraphNumber;
			averageEdgeNumberPerSubgraph = nEdge / subgraphNumber + 1;
		}

		std::cout << "The subgraph number: " << subgraphNumber << std::endl;
		std::cout << "Average Edge Number of Subgraph:" << averageEdgeNumberPerSubgraph << std::endl;

		/*
		 the datastruct which is needed in each subgraph: currentInEdges, array of vertices.
		*/
		int verticesPerSub = averageEdgeNumberPerSubgraph / AVERAGE_SCALE_OF_EDGE_VS_VERTICES;
		size_t memoryNeedForEachSub = verticesPerSub * sizeof(VertexDataType) + averageEdgeNumberPerSubgraph * sizeof(GraphInEdge);
		size_t leftAvailMemory =  totalAvailableMemory - minimalMemoryForEdgeAndData - memoryNeedForEachSub;


		size_t verticesMemory = nVertices * sizeof(VertexDataType);
		if (leftAvailMemory > verticesMemory) {

			// total vertices data can be loaded into memory.
			leftAvailMemory -= verticesMemory;
			mar.totalVertexDataInMemory = true;
			mar.forVertexData = 0;

			// check the degree data size
			size_t inDegreeDataSize = sizeof(int) * nVertices;
			size_t inDegreePrefixSumSize = inDegreeDataSize;
			if (bothDegreeUsed) {

				size_t degreeDataSize = inDegreeDataSize * 2;
				if (leftAvailMemory >= degreeDataSize) {
					mar.totalDegreeDataInMemory = true;
					mar.totalInDegreeDataInMemory = true;

					leftAvailMemory -= degreeDataSize;
					if (leftAvailMemory > inDegreePrefixSumSize) {
						
						mar.totalInDegreePrefixSumInMemory = true;
					} else {
						mar.totalInDegreePrefixSumInMemory = false;
					}

				} else {
					mar.totalDegreeDataInMemory = false;
					if (leftAvailMemory > inDegreeDataSize) {

						mar.totalInDegreeDataInMemory = true;
						mar.totalInDegreePrefixSumInMemory = false;

						mar.forOutDegreeData = (leftAvailMemory - inDegreeDataSize) * 0.7F;
					} else {
						mar.forOutDegreeData = leftAvailMemory * 0.7F;
					}
				}
			} else {
				// just use the indegree 
				if (leftAvailMemory >= inDegreeDataSize) {

					mar.totalInDegreeDataInMemory = true;
					leftAvailMemory -= inDegreeDataSize;

					if (leftAvailMemory > inDegreePrefixSumSize) {
						
						mar.totalInDegreePrefixSumInMemory = true;
					} else {
						mar.totalInDegreePrefixSumInMemory = false;
					}
				} 
			}

		} else {
			// set the bool member
			mar.totalVertexDataInMemory = false;
			mar.totalDegreeDataInMemory = false;
			mar.totalInDegreeDataInMemory = false;
			mar.totalInDegreePrefixSumInMemory = false;

			leftAvailMemory -= (verticesPerSub * sizeof(VertexDataType) + verticesPerSub * sizeof(int) * 2);
			if (bothDegreeUsed) {
				
				mar.forOutDegreeData = leftAvailMemory * 0.5F;
				mar.forVertexData = leftAvailMemory * 0.5F;
			} else {
				mar.forOutDegreeData = 0;
				mar.forVertexData = leftAvailMemory * 0.8F;
			}

		}

		mar.forEdgeAndEdgeData = minimalMemoryForEdgeAndData;
		mar.subgraphNumber = subgraphNumber;

		std::cout << mar.totalVertexDataInMemory << "  " << mar.totalDegreeDataInMemory << "  " << mar.totalInDegreeDataInMemory << "  ";
		std::cout << mar.totalInDegreePrefixSumInMemory << std::endl;

		std::cout << mar.forEdgeAndEdgeData << mar.subgraphNumber << std::endl;
		/*
		// Just for experiments.
		size_t forEE = 300 * 1024 * 1024; // memory for read Edge and Edge Data
		size_t forV = 400 * 1024 * 1024; // Vertex data
		size_t forD = 400 * 1024 * 1024; // Degree data
		size_t cacheForEdge = 100 * 1024 * 1024; // Cache Memory for Edge
		size_t cacheForEdgeData = 400 * 1024 * 1024; // Cache memory for edge data

		mar.cacheForEdge = cacheForEdge;
		mar.cacheForEdgeData = cacheForEdgeData;
		mar.forEdgeAndEdgeData = forEE;
		mar.forVertexData = forV;
		mar.forOutDegreeData = forD;
		mar.totalVertexDataInMemory = true;
		mar.totalDegreeDataInMemory = true;
		mar.totalInDegreeDataInMemory = true;
		mar.totalInDegreePrefixSumInMemory = true;
		*/
	}
}

#endif /* MEMORY_ALLOC_HPP_ */
