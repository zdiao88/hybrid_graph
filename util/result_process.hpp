/**
 * @author zhangdiao
 * @brief 
 * process result.
 */
#ifndef RESULT_PROCESS_HPP_
#define RESULT_PROCESS_HPP_

#include <iostream>
#include <string>
#include <cassert>
#include <cstdio>
#include <vector>
#include "min_heap.hpp"
#include "common.hpp" 


#define MAX_K (1 * 1024 * 1024)

struct VertexInfo{
	int vertexId;
	float vertexData;
	VertexInfo(){}
	VertexInfo(int _vertexId, float _vertexData):vertexId(_vertexId),vertexData(_vertexData){}
	bool operator<(VertexInfo other){
		return vertexData < other.vertexData;
	}
};

namespace graph{

	template <typename DataType>
	std::vector<VertexInfo> getTopK(std::string fileName, int k){
		if(k > MAX_K){
			std::cout << "Error: top " << k << "is too large, and only get the top "<< MAX_K << std::endl;
			k = MAX_K;
		}
		FILE *file = fopen(fileName.c_str(),"rb");

		int vertexNumber = getFileSize(fileName.c_str()) / sizeof(DataType);
		if(vertexNumber < k){
			std::cout << "Vertex number is smaller than " << k << std::endl;
			k = vertexNumber;
		}
		std::vector<VertexInfo> topK(k);
		DataType data;
		MinHeap<VertexInfo> mh(k);
		for(int i= 0; i < k; i ++){
			if(fread(&data,sizeof(DataType),1,file) == 1){
				mh.insert(VertexInfo(i,data));
			}
		}
		int index = k;
		while(fread(&data,sizeof(DataType),1,file) == 1){
			VertexInfo temp(index ++,data);
			if(mh.minElement() < temp){
				mh.setMinElement(temp);
			}
		}
		
		for(int i = k-1; i >=0; i --){
			VertexInfo data = mh.removeMin();
			topK[i] = data;
		}
		mh.release();

		return topK;
	}

}
#endif /* RESULT_PROCESS_HPP_ */
