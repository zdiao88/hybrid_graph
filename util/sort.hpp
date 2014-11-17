/**
 * @author zhangdiao
 * @brief 
 * ParseCmd: parse command line options.
 */

#ifndef SORT_HPP_
#define SORT_HPP_

#include "min_heap.hpp"
#include <iostream>

#define INSERTSORT_LIMIT 10

namespace graph{

	template <typename DataType>
	void insertSort(DataType *array, int number){
		for(int i = 1; i < number; i ++){
			int position = binaryFindForInsert(array,number,array[i]);
			if(i != position){
				DataType temp = array[i];
				for(int k = i; k > position; k --){
					array[k] = array[k-1];
				}
				array[position] = temp;
			}
		}
	}

	template<typename DataType>
	void insertSortFromEnd(DataType *array,int number){
		for(int i = 1; i < number; i ++){
			int j = i - 1;
			DataType temp = array[i];
			while(j >= 0 && temp < array[j]){
				array[j+1] = array[j];
				-- j;
			}

			if(j != i-1){
				array[j+1] = temp;
			}
		}
	}


	template <typename DataType>
	int binaryFindForInsert(DataType *array, int number, DataType &currentData){
		int low = 0,high = number - 1;
		int position = -1;
		while(low <= high){
			position = (low + high) / 2;
			if(currentData < array[position]){
				high = position - 1;
			}
			else{
				low = position + 1;
			}
		}
		if(currentData < array[position] || currentData == array[position]){
			++ position;
		}
		return position;
	}

	template <typename DataType>
	void swap(DataType *array, int src,int dst){
		DataType temp = array[src];
		array[src] = array[dst];
		array[dst] = temp;
	}

	template<typename DataType>
	void quickSort(DataType *array, int left,int right){
		if(left < right){
			int number = right - left + 1;
			if(number < INSERTSORT_LIMIT){
				insertSortFromEnd(array+left, number);
				return;
			}

			int i = left,j = right;
			int randIndex = left + rand() % number;
			swap<DataType>(array,left,randIndex);
			DataType pivot = array[left];
			while(i < j){
				while(i < j && pivot <= array[j])-- j;
				if(i < j)array[i++] = array[j];
				while(i < j && array[i] <= pivot) ++ i;
				if(i < j)array[j--] = array[i];
			}
			array[i] = pivot;

			quickSort(array,left,i-1);
			quickSort(array,i+1,right);
		}
	}

	template <typename DataType>
	void kMergeSort(DataType **arrays, int *elementNumber, int sourceNumber, DataType *sortedArray){
		MinHeap<MergeSortInfo<DataType> > mh(sourceNumber);
		int sortedIndex = 0;
		for(int i = 0; i < sourceNumber; i ++){
			if(elementNumber[i] < 0){
				-- sourceNumber;
				continue;
			}
			mh.insert(MergeSortInfo<DataType>(i,arrays[i][0]));
		}
		int *positions = (int *)malloc(sourceNumber * sizeof(int));
		for(int i = 0; i < sourceNumber; i ++){
			positions[i] = 0;
		}

		while(sourceNumber > 0 || mh.heapSize() != 0){
			MergeSortInfo<DataType> temp = mh.removeMin();
			sortedArray[sortedIndex ++] = temp.element;
			-- elementNumber[temp.arrayIndex];
			if(elementNumber[temp.arrayIndex] == 0){
				-- sourceNumber;
			}
			else{
				int index = temp.arrayIndex;
				mh.insert(MergeSortInfo<DataType>(index,arrays[index][++ positions[index]]));
			}
		}
		mh.release();
		free(positions);
	}

}
#endif /* SORT_HPP_ */
