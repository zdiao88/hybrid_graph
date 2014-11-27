/**
 * @author zhangdiao 
 * Sort graph file.
 */

#ifndef SORT_FILE_HPP_
#define SORT_FILE_HPP_

#include <cstdio>
#include "../util/graph_filename.hpp"
#include "../util/common.hpp"
#include "../util/min_heap.hpp"

#define MEMORY_FOR_SORT (1000 * 1024 * 1024)
#define WRITE_BUFFER_NUMBER (10 * 1024 * 1024)

namespace graph {

	template<typename DataType>
	class SortFile {

	private:
		DataType **loadedData; // loaded edge data in each temporary file.
		int *loadedDataNumber; // #edge in each loadedData.
		std::string fileName;
		int tempFileNumber;// the number of temporary file
		int *eachFileSize; // file size of each temp file
		int *eachFileDataNumber; // total number of edge in each file
		int loadedBudgetNumber; // budget edge number
		GraphFileName gfn;
		bool released;

	public:
		SortFile(int _tempFileNumber, std::string _fileName):fileName(_fileName), tempFileNumber(_tempFileNumber) {
			gfn.setBaseName(_fileName);
			loadedData = (DataType **)malloc(_tempFileNumber * sizeof(DataType *));
			// TODO: use size_t to replace int
			eachFileSize = (int *)malloc(_tempFileNumber * sizeof(int));
			eachFileDataNumber = (int *)malloc(_tempFileNumber * sizeof(int));
			loadedDataNumber = (int *)malloc(_tempFileNumber * sizeof(int));

			// get the file size and number of edge in each file.
			for (int i = 0; i < _tempFileNumber; i ++){
				eachFileSize[i] = (int)getFileSize(gfn.binaryTempFileName(i).c_str());
				eachFileDataNumber[i] = eachFileSize[i] / sizeof(DataType);
			}

			released =false;
		}

		~SortFile() {
			if (!released) {
				free(eachFileSize);
				free(loadedDataNumber);
				free(eachFileDataNumber);
				free(loadedData);

				for (int i = 0; i < tempFileNumber; i ++){
					free(loadedData[i]);
				}
			}

		}

		void releaseMemory() {

			if (!released) {

				free(eachFileSize);
				free(loadedDataNumber);
				free(eachFileDataNumber);
				
				for (int i = 0; i < tempFileNumber; i ++) {
					free(loadedData[i]);
				}

				free(loadedData);
				released = true;
			}
		}

		void sort() {

			mallocMemory();// malloc memory for each temporary file
			MinHeap<MergeSortInfo<DataType> > mh(tempFileNumber);
			// write the sorted file in file.
			int fd = open(gfn.sortedFileName().c_str(), O_CREAT|O_RDWR, 0777);
			int sortedIndex = 0;// edge index in buffer
			int sortedBytes = 0;// sorted bytes
			int writeDataSize = sizeof(DataType);
			DataType *sortedArray= (DataType *)malloc(WRITE_BUFFER_NUMBER * writeDataSize);
			int sourceNumber = tempFileNumber;
			//the index of each file
			int *positions = (int *)malloc(sourceNumber * sizeof(int));
			for (int i = 0; i < sourceNumber; i ++) {
				mh.insert(MergeSortInfo<DataType>(i, loadedData[i][0]));
				positions[i] = 0;
			}

			while (sourceNumber > 0 || mh.heapSize() != 0) {

				MergeSortInfo<DataType> temp = mh.removeMin();
				sortedArray[sortedIndex] = temp.element;
				sortedBytes += writeDataSize;
				if (++ sortedIndex == WRITE_BUFFER_NUMBER) {
					writeFile<DataType>(fd, sortedArray, sortedBytes);
					sortedBytes = 0;
					sortedIndex = 0;
				}

				int minIndex = temp.arrayIndex;
				-- loadedDataNumber[minIndex];
				if (loadedDataNumber[minIndex] == 0) {
					int leftDataNumber = eachFileDataNumber[minIndex];
					if (leftDataNumber > 0) {
						int loadNumber = (leftDataNumber < loadedBudgetNumber) ? leftDataNumber : loadedBudgetNumber;

						int fd = open(gfn.binaryTempFileName(minIndex).c_str(), O_RDONLY);
						int offset = eachFileSize[minIndex] - eachFileDataNumber[minIndex] * sizeof(DataType);
						readFile<DataType>(fd, loadedData[minIndex], loadNumber * sizeof(DataType), offset);
						close(fd);
						loadedDataNumber[minIndex] = loadNumber;
						eachFileDataNumber[minIndex] -= loadNumber;
						positions[minIndex] = 0;

						mh.insert(MergeSortInfo<DataType>(minIndex, loadedData[minIndex][positions[minIndex]]));
					} else {
						-- sourceNumber;
						remove(gfn.binaryTempFileName(minIndex).c_str());
					}
				} else {
					mh.insert(MergeSortInfo<DataType>(minIndex,loadedData[minIndex][++ positions[minIndex]]));
				}
			}

			if (sortedBytes > 0) {
				writeFile<DataType>(fd, sortedArray, sortedBytes);
			}
			close(fd);
			free(sortedArray);
			mh.release();
			free(positions);
			std::cout << "Finish sort graph." << std::endl;
			releaseMemory();
		}

	private:

		void mallocMemory() {
			// malloc memory to sort file.
			int dataSize = sizeof(DataType);
			int sortMemory = MEMORY_FOR_SORT;// the memory for sorting edge
			long cachedTotalEdges = sortMemory / dataSize;
			loadedBudgetNumber = cachedTotalEdges / tempFileNumber;
			long cacheMemoryEachSubgraph = loadedBudgetNumber * dataSize;

			int k = 0;
			for (k = 0; k < tempFileNumber; k ++) {
				if (eachFileSize[k] <= cacheMemoryEachSubgraph) {
					// if the file size is less than assigned memory
					loadedData[k] = (DataType *)malloc(eachFileSize[k]);
					loadedDataNumber[k] = eachFileSize[k] / dataSize;
				} else {
					loadedData[k] = (DataType *)malloc(cacheMemoryEachSubgraph);
					loadedDataNumber[k] = cacheMemoryEachSubgraph / dataSize;
				}

				eachFileDataNumber[k] -= loadedDataNumber[k];// update the number of left edges
				int fd = open(gfn.binaryTempFileName(k).c_str(), O_RDONLY);
				readFile<DataType>(fd, loadedData[k], loadedDataNumber[k] * dataSize, 0);
				close(fd);
			}
		}
	};
}

#endif /* SORT_FILE_HPP_ */
