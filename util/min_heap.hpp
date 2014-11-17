/**
 * @author zhangdiao
 * @brief 
 * MinHeap
 */

#include <cassert>

#ifndef KMERGE_HPP_
#define KMERGE_HPP_

namespace graph{


	template <typename DataType>
	struct MergeSortInfo{
		int arrayIndex;
		DataType element;
		MergeSortInfo(int _arrayIndex, DataType _element):arrayIndex(_arrayIndex),element(_element){}
		bool operator<(MergeSortInfo<DataType> &other){
			return element < other.element;
		}
	};
	template <typename DataType>
	class MinHeap{

	private:
		DataType *datas;
		int capacity;
		int size;
	public:
		MinHeap(){}
		MinHeap(int _capacity):capacity(_capacity),size(0){
			datas = (DataType *)calloc(capacity,sizeof(DataType));
		}
		~MinHeap(){
			if(datas != NULL)
				free(datas);
		}

		inline void release(){
			free(datas);
			datas = NULL;
		}
		inline int heapSize(){return size;}
		// 0 based.
		inline int parent(int i){return (i+1)/2 - 1;}
		inline int leftChild(int i){return i * 2 + 1;}
		inline int rightChild(int i){return i * 2 + 2;}

		void insert(DataType data){
			++ size;
			assert(size <= capacity);
			int pos = size - 1;
			for(;pos >= 1 && data < datas[parent(pos)]; pos = parent(pos)){
				datas[pos] = datas[parent(pos)];
			}
			datas[pos] = data;
		}

		inline DataType minElement(){
			assert(size > 0);
			return datas[0];
		}
		DataType removeMin(){
			DataType r = datas[0];
			-- size;
			datas[0] = datas[size];
			shiftDown(0);
			return r;
		}
		void setMinElement(DataType data){
			datas[0] = data;
			shiftDown(0);
		}
		void shiftDown(int i){
			int lc = leftChild(i);
			int rc = rightChild(i);
			int least = i;
			if(lc < size && datas[lc] < datas[least]) least = lc;
			if(rc < size && datas[rc] < datas[least]) least = rc;
			if(least != i){
				DataType temp = datas[least];
				datas[least] = datas[i];
				datas[i] = temp;
				shiftDown(least);
			}
		}
	};
}

#endif /* KMERGE_HPP_ */
