/**
 * @author zhangdiao
 * @brief 
 * The Graph Engine.
 */

#ifndef GRAPH_CORE_HPP_
#define GRAPH_CORE_HPP_

#include <string>
#include <vector>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <bitset>
#include <omp.h>
#include <math.h>
#include <stdlib.h>


#include "../util/common.hpp"
#include "../util/graph_filename.hpp"
#include "../util/time_record.hpp"

using namespace graph;

namespace graph {

	std::bitset<MAX_EDGE_NUMBER_PER_SUB> edgeFlag;

	Allocator alloc;
	volatile int ready;

	void  * fileIO(void * argument){
		GraphFileName gfn(alloc.fileName);
		ready = 0;
		int to_read_index = 0;
		int total_iter_loops = alloc.subgraphInterval.size()*alloc.niters;
		for(int i = 0; i < total_iter_loops; i ++){
			pthread_mutex_lock(&(alloc.globalLock));
			size_t usedMemory = alloc.usedMemory;
			pthread_mutex_unlock(&(alloc.globalLock));
			to_read_index = i %alloc.inEdgeSize.size();
				size_t 	needed_usedmemory = usedMemory+alloc.inEdgeSize[to_read_index]+alloc.inEdgeDataSize[to_read_index];
				if(needed_usedmemory < alloc.totalMemoryForEE){
					DataNode *p = (DataNode*)malloc(sizeof(DataNode));
					p ->next = NULL;

					size_t edge_offset = 0,edgedata_offset = 0;
					if(alloc.hasCached){
						if(alloc.hasMemoryToCacheEdge){
							edge_offset = alloc.cachedInEdgeSizeForEachSub[to_read_index];
						}

						if(alloc.edgeDataCached){
							edgedata_offset = alloc.cachedInEdgeDataSizeForEachSub[to_read_index];
						}
					}
					p ->edgePtr = (char*)malloc(alloc.inEdgeSize[to_read_index]-edge_offset);
					int in_fd = open(gfn.subgraphInEdgeFileName(alloc.subgraphInterval[to_read_index].first,alloc.subgraphInterval[to_read_index].second).c_str(),O_RDWR);
					readFile<char>(in_fd,p ->edgePtr,alloc.inEdgeSize[to_read_index]-edge_offset,edge_offset);
					close(in_fd);


					p ->edgeDataPtr = (char*)malloc(alloc.inEdgeDataSize[to_read_index]-edgedata_offset);
					if (alloc.readEdgeData && edgedata_offset < alloc.inEdgeDataSize[to_read_index]){
						int in_edgedata_fd = open(gfn.subgraphInEdgeDataFileName(alloc.subgraphInterval[to_read_index].first,alloc.subgraphInterval[to_read_index].second).c_str(),O_RDWR);
						readFile<char>(in_edgedata_fd,p ->edgeDataPtr,alloc.inEdgeDataSize[to_read_index]-edgedata_offset,edgedata_offset);
						close(in_edgedata_fd);
					}
					pthread_mutex_lock(&(alloc.globalLock));
					alloc.usedMemory += (alloc.inEdgeSize[to_read_index]+alloc.inEdgeDataSize[to_read_index]);
					(alloc.tail) ->next = p;
					alloc.tail = p;
					p = NULL;
					ready ++;
					pthread_mutex_unlock(&(alloc.globalLock));

					if(!alloc.hasCached && i == (int)alloc.subgraphInterval.size()-1){
						alloc.hasCached = true;
					}
			}
			else{
				--i;
			}
		}
		pthread_exit(NULL);
		return NULL;
	}

	template <typename VertexDataType, typename EdgeDataType>
	class GraphCore {

	public:
		typedef EdgeDataType (*InEdgeRelation)(VertexDataType *src, int outc, VertexDataType *dst, EdgeDataType *edgeData);
		typedef EdgeDataType (*OutEdgeRelation)(VertexDataType *dst, int inc, VertexDataType *src, EdgeDataType *edgeData);
	
	private:
		GraphFileName gfn; // graph name management
		GraphCoreInfo info; // state of core
		MemoryAllocResult mar; // memory allocation info
		TimeRecord tr;
		int startVertexId; // first vertex id.

	private:
		// structure of entire graph
		std::vector<GraphInterval> subgraphInterval; // subgraph interval
		int subgraphNumber; // #sub
		int totalVertexNumber; // #vertex
		int totalEdgeNumber; // #edge
		size_t *inEdgeSize; // array of subgraph's edge size
		size_t *inEdgeDataSize; // edge data size
		int iterNumber; // #iteration
		int iter; // current iteration
		bool needUpdateForEachVertex;
		bool useUnifiedVertexData; // whether init vertex data first or not
		VertexDataType unifiedData; // unified vertex data; e.g. pagerank = 0.15 firstly.
		VertexDataType initialVertexData;
		InEdgeRelation inRelation; // user defined rule for updating edge data
		bool cachedAfterFirstIter; // if the var == true, then can use cached data in next iteration.

	private:
		//structure of subgraph
		GraphInEdge *currentInEdge; // current in-edge
		GraphEdgeData<EdgeDataType> *currentInEdgeData; // in-edge data
		int currentSubgraph;
		int currentSubgraphStart; // start vertex id of current subgraph
		int currentSubgraphEnd;
		int currentSubgraphEdgeNumber;
		int *currentInDegreePrefixSum;
		int *currentInDegree; // indegree for current subgraph

	private:
		// bool: for optimization
		bool needModifyInEdge; // graph evolution
		bool readEdgeData;
		bool writeEdgeData;
		bool initialVertexDataFromFile; // file of initial vertex data
		bool initialVertexDataFirst; // initial vertex data before running
		bool hasMemoryToCacheEdge;
		bool hasMemoryToCacheEdgeData;
		bool needCacheForEdgeData;

		bool updateVertexDataWithEdgeData; // update edge and vertex data at the same time.e.g.pagerank = false, sgd = true

		/**
		 * algorithm may use both out and in degree or use in degree only
		 */
		bool useBothDegree;
		bool useInDegreeOnly;

		// entire data in memory
		bool allVertexDataInMemory;
		bool allInDegreePrefixSumInMemory;
		bool allDegreeDataInMemory;
		bool allInDegreeDataInMemory;

		bool initialVertexDataOutOfCore; // Also, it means initialize vertex data in preprocessing.

	private:
		VertexDataType *totalVertexData;
		int *totalInDegreePrefixSum;
		int *totalInDegree;
		int *totalOutDegree;

		/**
		 * vertex data can not be loaded in memory totally. make sure: current vertex data can
		 * be loaded in memory.
		 */
		size_t *vertexDataSize; // vertex data file size of each subgraph. only needed when total vertex data can't be loaded in memory
		VertexDataType *currentVertexData;
		VertexDataType **cachedVertexData; // software cache for subgraph's vertex data
		GraphInterval *cachedVertexDataInterval; // cache vertex interval of subgraph
		bool *vertexCacheHits; // true: total vertex data of a subgraph can be loaded in its cache.

		size_t *outDegreeDataSize;
		int **cachedForOutDegree;
		GraphInterval *cachedOutDegreeInterval;
		bool *outDegreeHits;

		size_t *subIndegreeFileSize;
		size_t *subIndegreePrefixSumFileSize;

	public:
		GraphCore(std::string fileName, const MemoryAllocResult &pMar) {
			mar = pMar;
			gfn.setBaseName(fileName);
			info.sameInitialVertexData = false; // if true, all the vertex is initialized with same data.
			tr.clear();// clear the time record object
			
			// get infomation from memory allocation result, which is done in preprocessing.
			subgraphNumber = mar.subgraphNumber;
			allVertexDataInMemory = mar.totalVertexDataInMemory;
			allDegreeDataInMemory = mar.totalDegreeDataInMemory;
			allInDegreeDataInMemory = mar.totalInDegreeDataInMemory;
			allInDegreePrefixSumInMemory = mar.totalInDegreePrefixSumInMemory;

			// data structure for each subgraph
			currentInEdge = NULL;
			currentInEdgeData = NULL;
			currentVertexData = NULL;
			currentInDegree = NULL;
			currentInDegreePrefixSum = NULL;
			cachedVertexData = NULL;
			cachedVertexDataInterval = NULL;
			vertexCacheHits = NULL;

			outDegreeDataSize = NULL;
			subIndegreeFileSize = NULL;
			subIndegreePrefixSumFileSize = NULL;

			inEdgeSize = NULL;
			inEdgeDataSize = NULL;
			vertexDataSize = NULL;

			totalVertexData = NULL;
			totalInDegree = NULL;
			totalOutDegree = NULL;
			totalInDegreePrefixSum = NULL;


			needModifyInEdge = false; // TODO: add the feature of graph evolution
			readEdgeData = true;
			writeEdgeData = false;
			initialVertexDataFromFile = false;
			initialVertexDataFirst = true;

			if (mar.cacheForEdge == 0) {
				hasMemoryToCacheEdge = false;
				alloc.hasMemoryToCacheEdge = false;
			} else {
				hasMemoryToCacheEdge = true;
				alloc.hasMemoryToCacheEdge = true;

				for (int i = 0; i < subgraphNumber; i ++) {
					alloc.cachedInEdgeSizeForEachSub.push_back(0);
				}
			}

			if (mar.cacheForEdgeData == 0) {
				hasMemoryToCacheEdgeData = false;
				alloc.hasMemoryToCacheEdgeData = false;
			} else {
				hasMemoryToCacheEdgeData = true;
				alloc.hasMemoryToCacheEdgeData = true;

				for (int i = 0; i < subgraphNumber; i ++) {
					alloc.cachedInEdgeDataSizeForEachSub.push_back(0);
				}
			}

			useBothDegree = true;
			useInDegreeOnly = false;
			needUpdateForEachVertex = true; // false: no execute process
			useUnifiedVertexData = false;
			needCacheForEdgeData = true;
			cachedAfterFirstIter = false;

			updateVertexDataWithEdgeData = false;

			// initial memory allocator
			alloc.readEdgeData = readEdgeData;
			alloc.head = (DataNode *)malloc(sizeof(DataNode));
			(alloc.head) -> next = NULL;
			alloc.tail = alloc.head;
			alloc.totalMemoryForEE = mar.forEdgeAndEdgeData;
			alloc.usedMemory = 0;
			alloc.fileName = fileName;
			alloc.edgeDataCached = false;
			pthread_mutex_init(&(alloc.globalLock), NULL);
			pthread_mutex_init(&(alloc.readWriteLock), NULL);

			initArray(mar.subgraphNumber);
			initialVertexDataOutOfCore = false;

			/*
				malloc.h:
				#define M_MMAP_THRESHOLD    -3
				#define M_MMAP_MAX          -4
			*/
			// mallopt(-4,0);
			// mallopt(-3,-1);
		}

		virtual ~GraphCore(){
			freeArray();
		}

		inline void initArray(int n) {

			inEdgeSize = (size_t*)malloc(n * sizeof(size_t));
			inEdgeDataSize = (size_t *)malloc(n * sizeof(size_t));
		}
		inline void freeArray() {

			if (inEdgeSize != NULL) {
				free(inEdgeSize);
				inEdgeSize = NULL;
			}

			if (inEdgeDataSize != NULL) {
				free(inEdgeDataSize);
				inEdgeDataSize = NULL;
			}

			if (cachedVertexDataInterval != NULL) {
				free(cachedVertexDataInterval);
				cachedVertexDataInterval = NULL;
			}

			if (vertexCacheHits != NULL) {
				free(vertexCacheHits);
				vertexCacheHits  = NULL;
			}

			if (vertexDataSize != NULL) {
				free(vertexDataSize);
				vertexDataSize = NULL;
			}
		}

		void run(ApplicationBase<VertexDataType, EdgeDataType> &app, int _iterNumber) {

			tr.startTime("runtime");
			std::cout << "======Start to Run Graph Core======" << std::endl;
			// load subgraph intervals
			std::cout << "Start to load subgraph interval." << std::endl;
			loadSubgraphInterval();
			startVertexId = subgraphInterval[0].first;
			totalVertexNumber = subgraphInterval[subgraphInterval.size() - 1].second + 1;
			// get the file size needed
			std::cout << "Start to load subgraph file info." << std::endl;
			fillFileSize();

			if (hasMemoryToCacheEdge) {
				allocCacheMemoryForEdge();
			}
			// has memory to cache edge data; algorithm need read edge data; cache for edge data
			if (hasMemoryToCacheEdgeData && readEdgeData && needCacheForEdgeData) {
				alloc.edgeDataCached = true;
				allocCacheMemoryForEdgeData();
			}

			std::cout << "Start to init vertex data." << std::endl;
			initVertexData(app);

			std::cout << "Start to init degree info." << std::endl;
			initDegreeData();
			initInDegreePrefixSumData();

			iterNumber = _iterNumber;
			alloc.niters = _iterNumber;

			// I/O thread
			pthread_t readFileT;
			pthread_create(&readFileT, NULL, fileIO, NULL);

			bool finishLastTime = true;

			for (iter = 0; iter < iterNumber; iter++) {

				std::cout << "Start Interation:" << iter << std::endl;
				for (int subgraphIndex = 0; subgraphIndex < subgraphNumber; subgraphIndex ++) {
					currentSubgraph = subgraphIndex;
					currentSubgraphStart = subgraphInterval[subgraphIndex].first;
					currentSubgraphEnd = subgraphInterval[subgraphIndex].second;
					std::cout << "Current Subgraph:" << currentSubgraphStart <<
						"---" << currentSubgraphEnd << std::endl;
					// nvertices in current subgraph
					int verticesNumber = currentSubgraphEnd - currentSubgraphStart + 1;

					if (!allInDegreeDataInMemory) {
						size_t fileSize = subIndegreeFileSize[currentSubgraph];
						currentInDegree = (int *)malloc(fileSize);
						int fd = open(gfn.subgraphInDegreeFileName(currentSubgraphStart, currentSubgraphEnd).c_str(), O_RDONLY);
						readFile<int>(fd, currentInDegree, fileSize, 0);
						close(fd);
					}

					if (!allInDegreePrefixSumInMemory) {
						size_t fileSize = subIndegreePrefixSumFileSize[currentSubgraph];
						currentInDegreePrefixSum = (int *)malloc(fileSize);
						int fd = open(gfn.subgraphInDegreePrefixSumFileName(currentSubgraphStart, currentSubgraphEnd).c_str(), O_RDONLY);
						readFile<int>(fd, currentInDegreePrefixSum, fileSize, 0);
						close(fd);
					}

					if (!allVertexDataInMemory) {
						if (vertexCacheHits[currentSubgraph]) {
							currentVertexData = cachedVertexData[currentSubgraph];
						} else {
							size_t dataSize = verticesNumber * sizeof(VertexDataType);
							currentVertexData = (VertexDataType *)malloc(dataSize);
							int vertexDataFd = open(gfn.subgraphVertexDataFileName(currentSubgraphStart, currentSubgraphEnd).c_str(), O_RDWR);
							readFile<VertexDataType>(vertexDataFd, currentVertexData, dataSize, 0);
							close(vertexDataFd);
						}
					}

					std::vector<GraphVertex<VertexDataType, EdgeDataType> > vertices(verticesNumber, GraphVertex<VertexDataType,EdgeDataType>());

					while (ready == 0 || !finishLastTime);

					finishLastTime = false;
					DataNode *currentReadyData = (alloc.head) ->next;
					currentSubgraphEdgeNumber = inEdgeSize[currentSubgraph] / sizeof(int);
					currentInEdge = (GraphInEdge *)malloc(currentSubgraphEdgeNumber * sizeof(GraphInEdge));

					if (alloc.edgeDataCached && cachedAfterFirstIter) {
						int edgeDataSize = sizeof(GraphEdgeData<EdgeDataType>);
						currentInEdgeData = (GraphEdgeData<EdgeDataType> *)malloc(currentSubgraphEdgeNumber * edgeDataSize);
						int offset = alloc.cachedInEdgeDataSizeForEachSub[currentSubgraph] / edgeDataSize;
						memcpy(currentInEdgeData, alloc.cachedEdgeData[currentSubgraph], alloc.cachedInEdgeDataSizeForEachSub[currentSubgraph]);
						memcpy(currentInEdgeData + offset, currentReadyData->edgeDataPtr, alloc.inEdgeDataSize[currentSubgraph] - alloc.cachedInEdgeDataSizeForEachSub[currentSubgraph]);
					} else {
						currentInEdgeData = (GraphEdgeData<EdgeDataType> *)(currentReadyData -> edgeDataPtr);
					}

					tr.startTime("initSubgraph");
					initSubgraph(vertices);
					tr.stopTime("initSubgraph");

					tr.startTime("execute_time");
					if(needUpdateForEachVertex){
						executeUpdate(app, vertices);
					}
					tr.stopTime("execute_time");

					if (!allInDegreeDataInMemory) {
						if (currentInDegree != NULL) {
							free(currentInDegree);
							currentInDegree = NULL;
						}
					}

					commitVertexDataAfterExecuteInterval();

					reloadOutDegreeData();

					commitEdgeDataAfterExecuteInterval();

					// update the data in other thread.
					pthread_mutex_lock(&(alloc.globalLock));
					ready --;
					DataNode* temp = (alloc.head) ->next;
					if (temp == alloc.tail) {
						alloc.tail = alloc.head;
					}
						
					(alloc.head)->next = temp ->next;
					free(temp ->edgePtr);
					free(temp->edgeDataPtr);
					free(temp);
					alloc.usedMemory -= (inEdgeSize[currentSubgraph] + inEdgeDataSize[currentSubgraph]);
					pthread_mutex_unlock(&(alloc.globalLock));

					edgeFlag.reset();
					finishLastTime = true;

					if (!allInDegreePrefixSumInMemory) {
						free(currentInDegreePrefixSum);
						currentInDegreePrefixSum = NULL;
					}
				} // finish interval

				cachedAfterFirstIter = true;

			} // finish iteration

			free(alloc.head);
			alloc.head = NULL;

			commitVertexDataAfterAllIteration();

			if (hasMemoryToCacheEdge) {
				for (int i = 0; i < subgraphNumber; i ++) {
					free(alloc.cachedInEdge[i]);
				}
				free(alloc.cachedInEdge);
			}

			if (alloc.edgeDataCached) {
				for (int i = 0; i < subgraphNumber; i ++) {
					free(alloc.cachedEdgeData[i]);
				}
				free(alloc.cachedEdgeData);
			}

			if (useBothDegree && allDegreeDataInMemory) {
				free(totalInDegree);
				totalInDegree = NULL;
				free(totalOutDegree);
				totalOutDegree = NULL;
			}

			if (useInDegreeOnly) {
				free(totalInDegree);
				totalInDegree = NULL;
			}
			
			std::cout << "finish run." << std::endl;
			tr.stopTime("runtime");
			tr.showTimes();
		}

		// load vertex interval of subgraph
		void loadSubgraphInterval() {

			std::string fileName = gfn.graphSubInterval();
			std::ifstream inf(fileName.c_str());
			if (!inf.good()) {
				std::cout << "Error: Can not open the file:" << gfn.graphSubInterval() << std::endl;
				return;
			}

			subgraphInterval.clear();
			int st = 0;
			int en = 0;
			inf >> st; // the first line is the start vertex id
			while (!inf.eof()) {
				inf >> en;
				if (st == en + 1) {
					break;
				}
				subgraphInterval.push_back(GraphInterval(st, en));
				st = en + 1;
			}
			inf.close();
			for (int i = 0; i < (int)subgraphInterval.size(); i++) {
				std::cout << subgraphInterval[i].first << "--" << subgraphInterval[i].second << std::endl;
			}
		}

		// get the file size of each structure
		void fillFileSize() {

			if (!allVertexDataInMemory) {
				vertexDataSize = (size_t*)malloc(subgraphNumber * sizeof(size_t));
			}

			if (useBothDegree && !allDegreeDataInMemory) {
				outDegreeDataSize = (size_t*)malloc(subgraphNumber * sizeof(size_t));
			}

			if (!allInDegreeDataInMemory) {
				subIndegreeFileSize = (size_t*)malloc(subgraphNumber * sizeof(size_t));
			}

			if (!allInDegreePrefixSumInMemory) {
				subIndegreePrefixSumFileSize = (size_t*)malloc(subgraphNumber * sizeof(size_t));
			}

			// set the array
			#pragma omp parallel for
			for (int snapIndex = 0; snapIndex < subgraphNumber; snapIndex ++) {

				int st = subgraphInterval[snapIndex].first;
				int en = subgraphInterval[snapIndex].second;

				int edgeSize = getFileSize(gfn.subgraphInEdgeFileName(st, en).c_str());
				int edgeDataSize = getFileSize(gfn.subgraphInEdgeDataFileName(st, en).c_str());
				inEdgeSize[snapIndex]=edgeSize;
				inEdgeDataSize[snapIndex] = edgeDataSize;

				if (vertexDataSize != NULL) {
					int fileSize = getFileSize(gfn.subgraphVertexDataFileName(st, en).c_str());
					vertexDataSize[snapIndex] = fileSize;
				}

				if (outDegreeDataSize != NULL) {
					int degreeSize = getFileSize(gfn.subgraphOutDegreeFileName(st, en).c_str());
					outDegreeDataSize[snapIndex]= degreeSize;
				}

				if (subIndegreeFileSize != NULL) {
					int size = getFileSize(gfn.subgraphInDegreeFileName(st, en).c_str());
					subIndegreeFileSize[snapIndex] = size;
				}

				if (subIndegreePrefixSumFileSize != NULL) {
					int size = getFileSize(gfn.subgraphInDegreePrefixSumFileName(st, en).c_str());
					subIndegreePrefixSumFileSize[snapIndex] = size;
				}
			}

			for (int i = 0; i < subgraphNumber; i ++) {
				alloc.subgraphInterval.push_back(subgraphInterval[i]);
				alloc.inEdgeSize.push_back(inEdgeSize[i]);
				alloc.inEdgeDataSize.push_back(inEdgeDataSize[i]);
			}
		}

		// allocator software cache for edge
		void allocCacheMemoryForEdge() {
			alloc.cachedInEdge = (char **)malloc(subgraphNumber * sizeof(char *));

			size_t cachedTotalEdges = mar.cacheForEdge / (sizeof(int) * 2);// srcId + outc
			size_t cacheMemoryEachSubgraph = cachedTotalEdges / subgraphNumber * sizeof(int);

			size_t tempSize = mar.cacheForEdge;
			int k = 0;
			for(k = 0; k < subgraphNumber; k ++){

				size_t tempEdgeSize = inEdgeSize[k] * 2;
				if (tempSize >= tempEdgeSize) {
					tempSize -= tempEdgeSize;
				} else {
					break;
				}
			}

			if (k == subgraphNumber) {
				for (int i = 0; i < subgraphNumber; i ++) {
					alloc.cachedInEdgeSizeForEachSub[i]  = inEdgeSize[i] ;
				}
			} else {
				size_t tempCacheSizeForEach = 0;
				for (int i = 0; i < subgraphNumber; i ++) {

					tempCacheSizeForEach = (inEdgeSize[i] > cacheMemoryEachSubgraph) ? cacheMemoryEachSubgraph : inEdgeSize[i];
					alloc.cachedInEdgeSizeForEachSub[i] = tempCacheSizeForEach;
				}
			}

			for (int i = 0; i < subgraphNumber; i ++) {
				alloc.cachedInEdge[i] = (char *)malloc(alloc.cachedInEdgeSizeForEachSub[i] * 2);
			}
		}

		// allocator software cache for edge data
		void allocCacheMemoryForEdgeData() {

			alloc.cachedEdgeData = (char **)malloc(subgraphNumber * sizeof(char *));
			size_t cachedTotalEdgeData = mar.cacheForEdgeData / sizeof(EdgeDataType);
			size_t cacheMemoryEdgedataEach = cachedTotalEdgeData / subgraphNumber * sizeof(EdgeDataType);

			int tempSize = mar.cacheForEdgeData;
			int k = 0;
			for (k = 0; k < subgraphNumber; k ++) {
				if (tempSize >= inEdgeDataSize[k]) {
					tempSize -= inEdgeDataSize[k];
				} else {
					break;
				}
			}

			if (k == subgraphNumber) {
				for (int i = 0; i < subgraphNumber; i ++) {
					alloc.cachedInEdgeDataSizeForEachSub[i]  = inEdgeDataSize[i] ;
				}
			} else {
				size_t tempCacheSizeForEach = 0;
				for (int i = 0; i < subgraphNumber; i ++) {
					tempCacheSizeForEach = inEdgeDataSize[i] > cacheMemoryEdgedataEach ? cacheMemoryEdgedataEach : inEdgeDataSize[i];
					alloc.cachedInEdgeDataSizeForEachSub[i] = tempCacheSizeForEach;
				}
			}

			for (int i = 0; i < subgraphNumber; i ++) {
				alloc.cachedEdgeData[i] = (char *)malloc(alloc.cachedInEdgeDataSizeForEachSub[i]);
			}
		}

		// initial vertex data
		void initVertexData(ApplicationBase<VertexDataType,EdgeDataType> &app) {

			if (allVertexDataInMemory) {

				totalVertexData = (VertexDataType *)malloc(totalVertexNumber * sizeof(VertexDataType));
				if (initialVertexDataOutOfCore) {
					initialVertexDataFromFile = false;
					initialVertexDataFirst = false;
				}

				if (initialVertexDataFromFile) {
					int vertexDataFD = open(gfn.graphVertexDataFileName().c_str(), O_RDONLY);
					size_t dataSize = totalVertexNumber * sizeof(VertexDataType);
					readFile<VertexDataType>(vertexDataFD, totalVertexData, dataSize, 0);
					close(vertexDataFD);
				} else if (initialVertexDataFirst) {
					if (!info.sameInitialVertexData) {
						#pragma omp parallel for
						for (int i = 0; i < totalVertexNumber; i ++) {
							totalVertexData[i] = app.initVertexData(info);
						}
					} else {
						#pragma omp parallel for
						for (int i = 0; i < totalVertexNumber; i ++) {
							totalVertexData[i] = initialVertexData;
						}
					}
					
				}
			} else {

				cachedVertexDataInterval = (GraphInterval *)malloc(subgraphNumber * sizeof(GraphInterval));
				vertexCacheHits = (bool *)malloc(subgraphNumber * sizeof(bool));

				size_t cachedVertexDataForEachSub = mar.forVertexData * 1.0f / subgraphNumber;
				int vertexSize = sizeof(VertexDataType);

				while (cachedVertexDataForEachSub % vertexSize != 0) {
					++ cachedVertexDataForEachSub;
				}

				int cachedVertexDataNum = cachedVertexDataForEachSub / vertexSize;
				cachedVertexData = (VertexDataType **)malloc(subgraphNumber * sizeof(VertexDataType *));

				#pragma omp parallel for
				for (int index = 0; index < subgraphNumber; index ++) {

					vertexCacheHits[index] = false;
					size_t tempMemory = cachedVertexDataForEachSub;
					int tempVertexNumber = cachedVertexDataNum;

					// subgraph's vertex data can be totally loaded in software cache
					if (vertexDataSize[index] <= cachedVertexDataForEachSub) {
						tempMemory = vertexDataSize[index];
						tempVertexNumber = tempMemory / vertexSize;
						vertexCacheHits[index] = true;
						cachedVertexDataInterval[index] = GraphInterval(subgraphInterval[index].first, subgraphInterval[index].second);
					}

					cachedVertexData[index] = (VertexDataType *)malloc(tempMemory);

					if (initialVertexDataFromFile) {
						int vertexdataFd = open(gfn.subgraphVertexDataFileName(subgraphInterval[index].first, subgraphInterval[index].second).c_str(), O_RDONLY);
						readFile<VertexDataType>(vertexdataFd, cachedVertexData[index], tempVertexNumber * sizeof(VertexDataType), 0);
						close(vertexdataFd);
					} else if (initialVertexDataFirst) {
						int subgraphVertexNumber = vertexDataSize[index] / sizeof(VertexDataType);
						VertexDataType *data = (VertexDataType *)malloc(vertexDataSize[index]);

						if(!info.sameInitialVertexData){
							for (int i = 0; i < subgraphVertexNumber; i ++) {
								data[i] = app.initVertexData(info);
							}	
						} else {
							for (int i = 0; i < subgraphVertexNumber; i ++) {
								data[i] = initialVertexData;
							}
							
						}
						
						int vertexdataFd = open(gfn.subgraphVertexDataFileName(subgraphInterval[index].first, subgraphInterval[index].second).c_str(), O_RDWR);
						writeFile<VertexDataType>(vertexdataFd, data, vertexDataSize[index]);
						close(vertexdataFd);

						memcpy(cachedVertexData[index], data , tempVertexNumber * sizeof(VertexDataType));
						free(data);
					}

					if (!vertexCacheHits[index]) {
						cachedVertexDataInterval[index] = GraphInterval(subgraphInterval[index].first, subgraphInterval[index].first + cachedVertexDataNum - 1);
					}
				}
			}
		}

		// load degree data
		void initDegreeData() {
			if (useBothDegree) {
				// assume in this case, useBothDegree means use all vertex's out degree + current
				// subgraph's indegree.
				if (allDegreeDataInMemory) {
					size_t degreeFileSize = totalVertexNumber * sizeof(int);
					totalInDegree = (int *)malloc(degreeFileSize);
					totalOutDegree = (int *)malloc(degreeFileSize);
					int inFd = open(gfn.graphInDegreeFileName().c_str(),O_RDONLY);
					int outFd = open(gfn.graphOutDegreeFileName().c_str(),O_RDONLY);

					readFile<int>(inFd, totalInDegree, degreeFileSize, 0);
					readFile<int>(outFd, totalOutDegree, degreeFileSize, 0);
					close(inFd);
					close(outFd);
				} else {
					// when allDegreeDataInMemory == false, the priority of in-degree > out-degree
					if (allInDegreeDataInMemory) {
						size_t degreeFileSize = totalVertexNumber * sizeof(int);
						totalInDegree = (int *)malloc(degreeFileSize);
						int inFd = open(gfn.graphInDegreeFileName().c_str(), O_RDONLY);
						readFile<int>(inFd, totalInDegree, degreeFileSize, 0);
						close(inFd);
					} else {
						// make sure the current subgraph's indegree can be loaded in memory
					}

					cachedForOutDegree = (int **)malloc(subgraphNumber * sizeof(int *));
					cachedOutDegreeInterval = (GraphInterval *)malloc(subgraphNumber * sizeof(GraphInterval));
					outDegreeHits = (bool *)malloc(subgraphNumber * sizeof(bool));

					size_t cachedDegreeDataForEachSub = mar.forOutDegreeData * 1.0f / subgraphNumber;
					int degreeDataSize = sizeof(int);

					while (cachedDegreeDataForEachSub % degreeDataSize != 0) {
						++ cachedDegreeDataForEachSub;
					}
					int cachedOutDegreeDataNum = cachedDegreeDataForEachSub / degreeDataSize;

					cachedForOutDegree = (int **)malloc(subgraphNumber*sizeof(int *));

					#pragma omp parallel for
					for (int index = 0; index < subgraphNumber; index ++) {
						outDegreeHits[index] = false;
						size_t tempMemory = cachedDegreeDataForEachSub;
						int tempVertexNumber = cachedOutDegreeDataNum;

						if (outDegreeDataSize[index] <= cachedDegreeDataForEachSub) {
							tempMemory = outDegreeDataSize[index];
							tempVertexNumber = tempMemory / degreeDataSize;
							outDegreeHits[index] = true;
							cachedOutDegreeInterval[index] = GraphInterval(subgraphInterval[index].first, subgraphInterval[index].second);
						}

						cachedForOutDegree[index] = (int *)malloc(tempMemory);

						int outDegreeFd = open(gfn.subgraphOutDegreeFileName(subgraphInterval[index].first, subgraphInterval[index].second).c_str(), O_RDONLY);
						readFile<int>(outDegreeFd, cachedForOutDegree[index], tempVertexNumber * sizeof(int), 0);
						close(outDegreeFd);

						if (!vertexCacheHits[index]) {
							cachedOutDegreeInterval[index] = GraphInterval(subgraphInterval[index].first, subgraphInterval[index].first + cachedOutDegreeDataNum - 1);
						}
					}
				}
			} else if (useInDegreeOnly) {
				if (allInDegreeDataInMemory) {
					size_t inDegreeFileSize = getFileSize(gfn.graphInDegreeFileName().c_str());
					totalInDegree = (int *)malloc(inDegreeFileSize);
					int inFd = open(gfn.graphInDegreeFileName().c_str(), O_RDONLY);
					readFile<int>(inFd, totalInDegree, inDegreeFileSize, 0);
					close(inFd);

				} else {
					// make sure the current subgraph's indegree can be loaded in memory
				}
			} else {
				// There is not case of using out-degree only in the in-edge model.
			}
		}

		void initInDegreePrefixSumData() {

			if (allInDegreePrefixSumInMemory) {
				size_t inDegreePrefixSumSize = getFileSize(gfn.graphInDegreePrefixSumFileName().c_str());
				totalInDegreePrefixSum = (int *)malloc(inDegreePrefixSumSize);
				int inFd = open(gfn.graphInDegreePrefixSumFileName().c_str(), O_RDONLY);
				readFile<int>(inFd,totalInDegreePrefixSum, inDegreePrefixSumSize, 0);
				close(inFd);

			} else {
				// only load subgraph's data before executing.
			}
		}

		void updateEdgeDataOutMemory(int degreeBaseVertexId, int *inDegree, std::vector<GraphVertex<VertexDataType, EdgeDataType> > &vertices) {

			size_t nvertices = vertices.size();
			bool specialProcess = (iter == 0 && useUnifiedVertexData);
			#pragma omp parallel for
			for (int i = 0; i <= subgraphNumber; i ++) {
				if (i == currentSubgraph) {
					int startVertex = currentSubgraphStart;
					int accInc = inDegree[startVertex - degreeBaseVertexId];
					for (int edgeIndex = 0; edgeIndex < currentSubgraphEdgeNumber; edgeIndex ++) {

						if (edgeIndex >= accInc) {
							++ startVertex;
							accInc += inDegree[startVertex - degreeBaseVertexId];
						}

						int vertex = currentInEdge[edgeIndex].srcId;
						int outc = currentInEdge[edgeIndex].outc;
						if (vertex >= currentSubgraphStart && vertex <= currentSubgraphEnd) {
							if (specialProcess) {
								currentInEdgeData[edgeIndex].edgedata = inRelation(&unifiedData, outc, &currentVertexData[startVertex - currentSubgraphStart],
									&(currentInEdgeData[edgeIndex].edgedata));
							} else {
								currentInEdgeData[edgeIndex].edgedata = inRelation(currentVertexData + (vertex - currentSubgraphStart), outc,
									&currentVertexData[startVertex - currentSubgraphStart], &(currentInEdgeData[edgeIndex].edgedata));
							}
							edgeFlag.set(edgeIndex, 1);
						}
					}
				} else if (i < subgraphNumber) {
					int st = subgraphInterval[i].first;
					int en = subgraphInterval[i].second;
					int startVertex = currentSubgraphStart;
					int accInc = inDegree[startVertex - degreeBaseVertexId];
					if (vertexCacheHits[i]) {
						for (int edgeIndex = 0; edgeIndex < currentSubgraphEdgeNumber; edgeIndex ++) {
							if (edgeIndex >= accInc) {
								++ startVertex;
								accInc += inDegree[startVertex- degreeBaseVertexId];
							}

							int vertex = currentInEdge[edgeIndex].srcId;
							int outc = currentInEdge[edgeIndex].outc;
							if (vertex >= st && vertex <= en && edgeFlag[edgeIndex] == 0) {
								if (specialProcess) {
									currentInEdgeData[edgeIndex].edgedata = inRelation(&unifiedData, outc, 
										&currentVertexData[startVertex-currentSubgraphStart], &(currentInEdgeData[edgeIndex].edgedata));
								} else {
									currentInEdgeData[edgeIndex].edgedata = inRelation(cachedVertexData[i] + (vertex - st), outc,
										&currentVertexData[startVertex-currentSubgraphStart], &(currentInEdgeData[edgeIndex].edgedata));
								}
								edgeFlag.set(edgeIndex, 1);
							}
						}
					} else {
						int cachedSt = cachedVertexDataInterval[i].first;
						int cachedEn = cachedVertexDataInterval[i].second;
						int cachedlength = cachedEn - cachedSt + 1;
						int j = 1;
						size_t offset = 0;
						do {
							for (int edgeIndex = 0; edgeIndex < currentSubgraphEdgeNumber; edgeIndex ++) {
								if (edgeIndex >= accInc) {
									++ startVertex;
									accInc += inDegree[startVertex- degreeBaseVertexId];
								}

								int vertex = currentInEdge[edgeIndex].srcId;
								int outc = currentInEdge[edgeIndex].outc;
								if (vertex >= cachedSt && vertex <= cachedEn && edgeFlag[edgeIndex] == 0) {

									if (specialProcess) {
										currentInEdgeData[edgeIndex].edgedata = inRelation(&unifiedData, outc,
											&currentVertexData[startVertex - currentSubgraphStart], &(currentInEdgeData[edgeIndex].edgedata));
									}
									else{
										currentInEdgeData[edgeIndex].edgedata = inRelation(cachedVertexData[i] + (vertex - cachedSt), outc, 
											&currentVertexData[startVertex-currentSubgraphStart], &(currentInEdgeData[edgeIndex].edgedata));
									}
									edgeFlag.set(edgeIndex, 1);
								}
							}

							if (updateVertexDataWithEdgeData) {
								size_t datasize = (cachedEn - cachedSt + 1) * sizeof(VertexDataType);
								int vertexdataFd = open(gfn.subgraphVertexDataFileName(currentSubgraphStart,currentSubgraphEnd).c_str(),O_RDWR);
								writeFileWithOffset<VertexDataType>(vertexdataFd, currentVertexData, datasize, offset);
								close(vertexdataFd);
							}
							cachedSt = cachedEn + 1;
							cachedEn = (en - cachedEn > cachedlength)? cachedSt - 1 + cachedlength:en;

							if (cachedSt <= en) {
								offset= j * (cachedlength * sizeof(VertexDataType));
								size_t datasize = (cachedEn - cachedSt + 1) * sizeof(VertexDataType);
								int vertexdataFd = open(gfn.subgraphVertexDataFileName(st, en).c_str(), O_RDONLY);
								readFile<VertexDataType>(vertexdataFd, cachedVertexData[i], datasize, offset);
								close(vertexdataFd);
								++j;
							}
						} while (cachedSt <= en);
					}
				} else if (i == subgraphNumber) {
					int inEdgeCount = 0;
					for (int i=0; i < (int)nvertices; i++) {
						int inc = inDegree[currentSubgraphStart + i - degreeBaseVertexId];
						vertices[i] = GraphVertex<VertexDataType, EdgeDataType>(currentSubgraphStart + i, currentVertexData+i, inc, 
							(currentInEdge + inEdgeCount), (currentInEdgeData + inEdgeCount));
						inEdgeCount += inc;
					}
				}
			}
		}

		void updateEdgeDataInMemory(int vertexBaseId, int degreeBaseVertexId, int inDegreePrefixVertexId, int *inDegree, int *inDegreePrefixSum,
					std::vector<GraphVertex<VertexDataType,EdgeDataType> > &vertices) {

			size_t nvertices = vertices.size();
			bool specialProcess = (iter == 0 && useUnifiedVertexData);
			#pragma omp parallel for
			for (int block = 0; block < (int)nvertices; block ++) {

				int startEdgeIndex = inDegreePrefixSum[inDegreePrefixVertexId + block];
				int inc = inDegree[degreeBaseVertexId + block];
				for (int i = 0; i < inc; i ++) {
					int vertex = currentInEdge[startEdgeIndex + i].srcId;
					int outc = currentInEdge[startEdgeIndex + i].outc;
					if (specialProcess) {
						currentInEdgeData[startEdgeIndex + i].edgedata = inRelation(&unifiedData, outc, &totalVertexData[vertexBaseId + block], 
							&(currentInEdgeData[startEdgeIndex + i].edgedata)); 
					} else {
						currentInEdgeData[startEdgeIndex + i].edgedata = inRelation(totalVertexData + vertex, outc, &totalVertexData[vertexBaseId + block],
							&(currentInEdgeData[startEdgeIndex + i].edgedata));
					}
				}
				vertices[block] = GraphVertex<VertexDataType,EdgeDataType>(vertexBaseId + block, totalVertexData + (vertexBaseId + block), inc, 
					(currentInEdge + startEdgeIndex), (currentInEdgeData + startEdgeIndex));
			}
		}


		void initSubgraph(std::vector<GraphVertex<VertexDataType,EdgeDataType> > &vertices) {

			DataNode* currentReadyData = (alloc.head) -> next;
			int *edges = (int *)(currentReadyData -> edgePtr);

			int startIndex = 0;
			if (cachedAfterFirstIter && hasMemoryToCacheEdge) {
				startIndex = alloc.cachedInEdgeSizeForEachSub[currentSubgraph] / sizeof(int);
				memcpy(currentInEdge, alloc.cachedInEdge[currentSubgraph], startIndex * sizeof(GraphInEdge));
			}

			if (useBothDegree) {
				if (allDegreeDataInMemory) {
					#pragma omp parallel for
					for (int i = startIndex; i < currentSubgraphEdgeNumber; i ++) {
						int vertexId = edges[i - startIndex];
						currentInEdge[i].srcId = vertexId;
						currentInEdge[i].outc = totalOutDegree[vertexId];
					}
				} else {
					#pragma omp parallel for
					for (int i = 0; i <= subgraphNumber; i ++) {
						if (outDegreeHits[i]) {
							int startI = subgraphInterval[i].first;
							int endI = subgraphInterval[i].second;
							for (int edgeIndex = startIndex; edgeIndex < currentSubgraphEdgeNumber; edgeIndex ++) {
								int vertexId = edges[edgeIndex - startIndex];
								currentInEdge[edgeIndex].srcId = vertexId;
								if (vertexId >= startI && vertexId <= endI && edgeFlag[edgeIndex] == 0) {
									currentInEdge[edgeIndex].outc = cachedForOutDegree[i][vertexId - startI];
									edgeFlag.set(edgeIndex, 1);
								}
							}
						} else {
							int startI = subgraphInterval[i].first;
							int endI = subgraphInterval[i].second;
							int cachedSt = cachedOutDegreeInterval[i].first;
							int cachedEn = cachedOutDegreeInterval[i].second;
							int cachedlength = cachedEn - cachedSt + 1;
							int j = 1;
							size_t offset = 0;
							do {
								for (int edgeIndex = startIndex; edgeIndex < currentSubgraphEdgeNumber; edgeIndex ++) {
									int vertexId = edges[edgeIndex - startIndex];
									currentInEdge[edgeIndex].srcId = vertexId;
									if (vertexId >= cachedSt && vertexId <= cachedEn && edgeFlag[edgeIndex] == 0) {
										currentInEdge[edgeIndex].outc = cachedForOutDegree[i][vertexId - startI];
										edgeFlag.set(edgeIndex, 1);
									}
								}
								cachedSt = cachedEn + 1;
								cachedEn = (endI - cachedEn > cachedlength) ? cachedSt - 1 + cachedlength : endI;

								if (cachedSt <= endI) {
									offset= j * (cachedlength * sizeof(int));
									size_t datasize = (cachedEn - cachedSt + 1) * sizeof(int);
									int outDegreeFd = open(gfn.subgraphOutDegreeFileName(startI, endI).c_str(), O_RDWR);
									readFile<int>(outDegreeFd, cachedForOutDegree[i], datasize, offset);
									close(outDegreeFd);
									++j;
								}

							} while (cachedSt <= endI);

						}
					}
				}
			} else {
				#pragma omp parallel for
				for(int i = startIndex; i < currentSubgraphEdgeNumber; i ++){
					int vertexId = edges[i - startIndex];
					currentInEdge[i].srcId = vertexId;
				}
			}

			if (!cachedAfterFirstIter) {
				if (hasMemoryToCacheEdge) {
					int temp = alloc.cachedInEdgeSizeForEachSub[currentSubgraph] / sizeof(int);
					memcpy(alloc.cachedInEdge[currentSubgraph], currentInEdge, temp * sizeof(GraphInEdge));
				}

				if (alloc.edgeDataCached) {
					int tempEdgeData = alloc.cachedInEdgeDataSizeForEachSub[currentSubgraph] / sizeof(EdgeDataType);
					memcpy(alloc.cachedEdgeData[currentSubgraph], currentInEdgeData, tempEdgeData * sizeof(GraphEdgeData<EdgeDataType>));
				}
			}

			edgeFlag.reset();
			int st = currentSubgraphStart;
			if (allVertexDataInMemory) {
				if (allInDegreeDataInMemory) {
					if (allInDegreePrefixSumInMemory) {
						updateEdgeDataInMemory(st, st - startVertexId, st - startVertexId, totalInDegree, totalInDegreePrefixSum, vertices);
					}
					else {
						updateEdgeDataInMemory(st, st-startVertexId, 0, totalInDegree, currentInDegreePrefixSum, vertices);
					}
				} else {
					if (allInDegreePrefixSumInMemory) {
						// the priority of in-degree > in-degree prefix, so this scope will never cover.
						updateEdgeDataInMemory(st, 0, st-startVertexId, currentInDegree, totalInDegreePrefixSum, vertices);
					} else {
						updateEdgeDataInMemory(st, 0, 0, currentInDegree, currentInDegreePrefixSum, vertices);
					}
				}
			} else {
				if (allInDegreeDataInMemory) {
					updateEdgeDataOutMemory(0, totalInDegree, vertices);
				} else {
					updateEdgeDataOutMemory(st, currentInDegree, vertices);
				}
			}
		}

		void executeUpdate(ApplicationBase<VertexDataType, EdgeDataType> &app,
				std::vector<GraphVertex<VertexDataType,EdgeDataType> > &vertices) {

			int verticesNumber = currentSubgraphEnd - currentSubgraphStart + 1;
			#pragma omp parallel for schedule(dynamic)
			for (int i = 0; i < verticesNumber; i ++) {
				GraphVertex<VertexDataType,EdgeDataType> &vertex = vertices[i];
				app.updateVertexModel(vertex, info);
			}
		}

		void commitVertexDataAfterExecuteInterval() {
			if (!allVertexDataInMemory) {
				 for (int snapIndex = 0; snapIndex < subgraphNumber; snapIndex ++) {
					 if (!vertexCacheHits[snapIndex]) {
						 size_t cacheDataSize = (cachedVertexDataInterval[snapIndex].second - cachedVertexDataInterval[snapIndex].first + 1) * sizeof(VertexDataType);
						 if (snapIndex == currentSubgraph) {
							 memcpy(cachedVertexData[currentSubgraph], currentVertexData, cacheDataSize);
							 size_t datasize = (currentSubgraphEnd - currentSubgraphStart + 1) * sizeof(VertexDataType);
							 int vertexdataFd = open(gfn.subgraphVertexDataFileName(currentSubgraphStart, currentSubgraphEnd).c_str(), O_RDWR);
							 writeFile<VertexDataType>(vertexdataFd, currentVertexData, datasize);
							 close(vertexdataFd);
						 } else {
							 int vertexdataFd = open(gfn.subgraphVertexDataFileName(subgraphInterval[snapIndex].first, subgraphInterval[snapIndex].second).c_str(), O_RDWR);
							 readFile<VertexDataType>(vertexdataFd, cachedVertexData[snapIndex], cacheDataSize, 0);
							 close(vertexdataFd);
						 }
					 }

				 }

				if (!vertexCacheHits[currentSubgraph]) {
					 free(currentVertexData);
					 currentVertexData = NULL;
				 }
			 }
		}

		void commitEdgeDataAfterExecuteInterval() {
			DataNode *currentReadyData = (alloc.head) ->next;
			size_t edgeOffSet = 0;
			if (needModifyInEdge) {
				 if (hasMemoryToCacheEdge && !cachedAfterFirstIter) {
					 edgeOffSet = alloc.cachedInEdgeSizeForEachSub[currentSubgraph];
					 int tempFd0 = open(gfn.subgraphInEdgeFileName(currentSubgraphStart,currentSubgraphEnd).c_str(),O_RDWR);
					 writeFile<char>(tempFd0,currentReadyData ->edgePtr,edgeOffSet);
					 close(tempFd0);
				 }

				 int tempFd0 = open(gfn.subgraphInEdgeFileName(currentSubgraphStart,currentSubgraphEnd).c_str(),O_RDWR);
				 writeFileWithOffset<char>(tempFd0,currentReadyData ->edgePtr,inEdgeSize[currentSubgraph]-edgeOffSet,edgeOffSet);
				 close(tempFd0);
			 }
			 free(currentInEdge);
			 currentInEdge = NULL;

			 if (writeEdgeData) {
				 int tempFd0 = open(gfn.subgraphInEdgeDataFileName(currentSubgraphStart, currentSubgraphEnd).c_str(), O_RDWR);
				 writeFile<GraphEdgeData<EdgeDataType> >(tempFd0, currentInEdgeData, inEdgeDataSize[currentSubgraph]);
				 close(tempFd0);
			 }

			 if (alloc.edgeDataCached && cachedAfterFirstIter) {
				 free(currentInEdgeData);
			 }
			 currentInEdgeData = NULL;
		}

		void commitVertexDataAfterAllIteration() {
			if (allVertexDataInMemory) {
				size_t datasize = getFileSize(gfn.graphVertexDataFileName().c_str());
				int vertexdataFd = open(gfn.graphVertexDataFileName().c_str(), O_RDWR);
				writeFile<VertexDataType>(vertexdataFd, totalVertexData, datasize);
				close(vertexdataFd);
				free(totalVertexData);
				totalVertexData = NULL;
			} else {
				#pragma omp parallel for schedule(dynamic)
				for (int snapIndex = 0; snapIndex < subgraphNumber; snapIndex ++) {
					if (vertexCacheHits[snapIndex]) {
						 size_t datasize = (subgraphInterval[snapIndex].second - subgraphInterval[snapIndex].first + 1) * sizeof(VertexDataType);
						 int vertexdataFd = open(gfn.subgraphVertexDataFileName(subgraphInterval[snapIndex].first, subgraphInterval[snapIndex].second).c_str(), O_RDWR);
						 writeFile<VertexDataType>(vertexdataFd, cachedVertexData[snapIndex], datasize);
						 free(cachedVertexData[snapIndex]);
						 close(vertexdataFd);
					}
				}

				if (cachedVertexData != NULL) {
					free(cachedVertexData);
					cachedVertexData = NULL;
				}
			}
		}

		void reloadOutDegreeData() {
			if (useBothDegree && !allDegreeDataInMemory) {
				 for (int snapIndex = 0; snapIndex < subgraphNumber; snapIndex ++) {
					 if (!outDegreeHits[snapIndex]) {
						 size_t cacheDataSize = (cachedOutDegreeInterval[snapIndex].second - cachedOutDegreeInterval[snapIndex].first + 1) * sizeof(int);
						 int outDegreeFd = open(gfn.subgraphOutDegreeFileName(subgraphInterval[snapIndex].first, subgraphInterval[snapIndex].second).c_str(), O_RDONLY);
						 readFile<int>(outDegreeFd,cachedForOutDegree[snapIndex], cacheDataSize, 0);
						 close(outDegreeFd);
					 }
				 }
			}
		}
		inline void setModifiesInEdge(bool b) {
			needModifyInEdge = b;
		}
		inline void setReadEdgeData(bool b) {
			readEdgeData = b;
			alloc.readEdgeData = readEdgeData;
		}
		inline void setWriteEdgeData(bool b) {
			writeEdgeData = b;
		}
		inline void initialVertexOutOfCore(){
			initialVertexDataOutOfCore = true;
		}
		inline void initialVertexFromFile(){
			initialVertexDataFromFile = true;
			initialVertexDataFirst = false;
		}
		inline void initialVertexFromApp(){
			initialVertexDataFirst = true;
			initialVertexDataFromFile = false;
		}
		inline void setUpdateVertexDataWithEdgeData(bool b){
			updateVertexDataWithEdgeData = b;
		}
		inline void setUnifiedData(VertexDataType initialData){
			unifiedData = initialData;
			useUnifiedVertexData = true;
		}
		inline void setInitialVertexData(VertexDataType initialData){
			initialVertexData = initialData;
		}
		inline void setUseInDegreeOnly(){
			useBothDegree = false;
			useInDegreeOnly = true;
		}
		inline void setNeedExcuteUpdateForVertex(bool b){
			needUpdateForEachVertex = b;
		}
		inline void setNeedCacheForEdgeData(bool b){
			needCacheForEdgeData = b;
		}
		inline void setInEdgeRelation(InEdgeRelation r){
			inRelation = r;
		}
	};
}

#endif /* GRAPH_CORE_HPP_ */
