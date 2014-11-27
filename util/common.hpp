/**
 * @author zhangdiao
 * frequently-used struct and function.
 */

#ifndef COMMON_HPP_
#define COMMON_HPP_

#include <iostream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <vector>
#include <cstring>

#define MAX_EDGE_NUMBER_PER_SUB (50 * 1024 * 1024)

enum GraphOrder {
	SOURCE_ORDER,
	DESTINATION_ORDER
};

typedef long unsigned int long_t;
typedef int vertex_t;
typedef long edge_t;

namespace graph{

	struct GraphInterval{
		int first;
		int second;
		GraphInterval(int _first, int _second):first(_first), second(_second) {}
	};

	struct GraphDegree{
		int inDegree;
		int outDegree;
		GraphDegree(int _in,int _out):inDegree(_in), outDegree(_out) {}
	};
	struct DataNode {
		char *edgePtr;
		char *edgeDataPtr;
		DataNode *next;
	};

	template <typename EdgeDataType>
	struct EdgeWithValue {

		int src;
		int dst;
		EdgeDataType edgeValue;
		EdgeWithValue<EdgeDataType>() {}
		EdgeWithValue(int _src, int _dst, EdgeDataType _edgeValue):src(_src), dst(_dst), edgeValue(_edgeValue) {}

		bool operator< (EdgeWithValue<EdgeDataType> &other) {
			return dst < other.dst;
		}

		bool operator<= (EdgeWithValue<EdgeDataType> &other) {
			return dst < other.dst;
		}

		bool operator== (EdgeWithValue<EdgeDataType> &other) {
			return dst == other.dst;
		}
	};

	struct Allocator{
		size_t totalMemoryForEE;
		size_t usedMemory;
		std::vector<size_t> inEdgeSize;
		std::vector<size_t> inEdgeDataSize;
		int niters;
		std::string fileName;
		std::vector<GraphInterval> subgraphInterval;
		pthread_mutex_t globalLock;
		pthread_mutex_t readWriteLock;
		DataNode *head;
		DataNode *tail;

		char ** cachedInEdge;
		char ** cachedEdgeData;
		bool edgeDataCached;

		bool hasCached;
		std::vector<size_t> cachedInEdgeSizeForEachSub;
		std::vector<size_t> cachedInEdgeDataSizeForEachSub;
		bool readEdgeData;

		bool hasMemoryToCacheEdge;
		bool hasMemoryToCacheEdgeData;
	};

	template<typename EdgeDataType>
	EdgeWithValue<EdgeDataType> parseGraphLine(char *line) {
		
		char splitChar[] = "\t, ";
		char * data;
		data = strtok(line, splitChar);
		int src = atoi(data);

		data = strtok(NULL, splitChar);
		int dst = atoi(data);

		data = strtok(NULL, splitChar);
		float val = 0.0F;
		if (data != NULL) {
			val = atof(data);;
		}

		return EdgeWithValue<EdgeDataType>(src, dst, val);
	}

	void removeN(char *str){
		int length = (int)strlen(str);
		if(str[length - 1] == '\n'){
			str[length - 1] = 0;
		}
	}

	template <typename T>
	void writeFile(int fd, T *buffer, size_t fileSize){
		size_t nwritten = 0;
		char * buf = (char*)buffer;
		while(nwritten<fileSize) {
			size_t a = write(fd, buf, fileSize-nwritten);
			assert(a>0);
			buf += a;
			nwritten += a;
		}
	}
	template <typename T>
	void writeFileWithOffset(int fd, T *buffer, size_t fileSize,size_t offset){
		size_t nwritten = 0;
		char * buf = (char*)buffer;
		while(nwritten<fileSize) {
			size_t a = pwrite(fd, buf, fileSize-nwritten,offset+nwritten);
			assert(a>0);
			buf += a;
			nwritten += a;
		}
	}

	template <typename T>
	void readFile(int fd, T * buffer, size_t fileSize, size_t offset) {
	    size_t nread = 0;
	    char * buf = (char*)buffer;
	    while(nread<fileSize) {
	        ssize_t a = pread(fd, buf, fileSize - nread, offset + nread);
	        assert(a>0);
	        buf += a;
	        nread += a;
	    }
	    assert(nread <= fileSize);
	}
	unsigned long getFileSize(const char *fileName)
	{
		int f = open(fileName, O_RDONLY);
		if (f < 0) {
			std::cout << "Error: can not open the file." << fileName << std::endl;
			assert(false);
		}
		off_t sz = lseek(f, 0, SEEK_END);
		close(f);
		return sz;
	}

}
#endif /* COMMON_HPP_ */
