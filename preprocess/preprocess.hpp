/**
 * @author zhangdiao
 * @brief 
 * Preprocessing graph file.
 */

#ifndef PREPROCESS_HPP_
#define PREPROCESS_HPP_
#include "../util/memory_alloc.hpp"
#include "../util/parse_config.hpp"
#include "../util/sort.hpp"
#include "sort_file.hpp"
#include <vector>
#include <fstream>
#include <cstdio>
#include <cassert>

namespace graph{

	template <typename VertexDataType, typename EdgeDataType>
	class GraphConversion{

	private:
		std::string fileName;
		GraphFileName gfn;// manage graph file name
		std::vector<GraphInterval> subgraphIntervals;
		vertex_t totalVertexNumber;
		edge_t totalEdgeNumber;

		// byte per edge,edge data, and indegree
		int eachEdgeByte;
		int eachEdgeDataByte;
		int indegreeByte;
		// buffer for preprocessing
		int *inEdgeBuffer;
		int *indegreeBuffer;
		int *indegreePrefixSumBuffer;
		int *outdegreeBuffer;
		EdgeDataType *edgeDataBuffer;

		// file descriptor for edge, edge data, in degree and prefix sum (subgraph)
		int inFd;
		int inEdgeDataFd;
		int indegreeFd;
		int indegreePrefixFd;

		// degree and prefix sum file descriptor for entire graph
		int totalInDegreeFd;
		int totalInDegreePrefixSumFd;

		vertex_t limitVertexNumber;
		// buffered data bytes
		long_t inNbytes;// in edge size in byte
		long_t inEdgedataNbytes;// edge data size in byte
		long_t indegreeNbytes;
		long_t outdegreeNbytes;// outdegree bytes

		edge_t indegreeAcc;// the accumulated indegree
		int bufferedDataNumber;// the number of vertex or edge in buffer
		int vertexCount; // index for indegreeBuffer
		int edgeCount;// count the number of edge in buffer and flush to file when equals n
		vertex_t preid;// previous vertex id
		vertex_t zeroId;// the vacant vertex id
		vertex_t indegreeCount;// the in degree of a vertex
		long_t leftEdgeNumber;// left edges to be processed
		vertex_t startId;// start vertex id
		int edgeNumberEach;// edge number in each subgraph
		int totalCountEach;// the number of edge in each subgraph
		vertex_t src;
		vertex_t dst;
		EdgeDataType val;

		bool initialVertexDataAlso;
		bool bufferReleased;

	public:
		GraphConversion(std::string _fileName):fileName(_fileName){
			gfn.setBaseName(fileName);
			bufferedDataNumber = 10*1024*1024;
			eachEdgeByte = sizeof(int);// store the src vertexid for inedge only
			eachEdgeDataByte = sizeof(EdgeDataType);
			indegreeByte = sizeof(int);

			inNbytes = 0;
			inEdgedataNbytes = 0;
			indegreeNbytes = 0;
			outdegreeNbytes = 0;

			indegreeAcc = 0;
			edgeCount = 0;
			vertexCount = 0;
			preid = -1;
			zeroId = 0;
			indegreeCount = 0;
			leftEdgeNumber = 0;
			startId = 0;
			totalCountEach = 0;
			bufferReleased = false;
			initialVertexDataAlso = false;

			inEdgeBuffer = (int *)malloc(bufferedDataNumber * eachEdgeByte);
			indegreeBuffer = (int *)malloc(bufferedDataNumber * indegreeByte);
			outdegreeBuffer = (int *)malloc(bufferedDataNumber * indegreeByte);
			indegreePrefixSumBuffer = (int *)malloc((bufferedDataNumber+1)*sizeof(int));
			indegreePrefixSumBuffer[0] = 0;// in degree prefix sum of first vertex in each subgraph equals 0
			edgeDataBuffer = (EdgeDataType*)malloc(bufferedDataNumber * eachEdgeDataByte);
		}

		~GraphConversion(){
			if(!bufferReleased){
				free(inEdgeBuffer);
				free(indegreeBuffer);
				free(outdegreeBuffer);
				free(indegreePrefixSumBuffer);
				free(edgeDataBuffer);
			}
		}

		void resetBufferBytes(){
			inNbytes = 0;
			inEdgedataNbytes = 0;
			indegreeNbytes = 0;
			outdegreeNbytes = 0;

			indegreeAcc = 0;
			edgeCount = 0;
			vertexCount = 0;
			preid = -1;
			zeroId = 0;
			indegreeCount = 0;
			leftEdgeNumber = 0;
			startId = 0;
			totalCountEach = 0;

			indegreePrefixSumBuffer[0] = 0;
		}

		void releaseBuffers(){
			if(!bufferReleased){
				free(inEdgeBuffer);inEdgeBuffer = NULL;
				free(indegreeBuffer);indegreeBuffer = NULL;
				free(indegreePrefixSumBuffer);indegreePrefixSumBuffer = NULL;
				free(edgeDataBuffer);edgeDataBuffer = NULL;
				bufferReleased = true;
			}
		}
		inline GraphFileName getGFN(){return gfn;}

		void createTempEdgeFile(){
			// create file
			inFd = open(gfn.tempFileName("_edge_temp").c_str(),O_CREAT|O_RDWR,0777);
			inEdgeDataFd = open(gfn.tempFileName("_edgedata_temp").c_str(),O_CREAT|O_RDWR,0777);
		}
		void closeTempEdgeFile(){
			close(inFd);
			close(inEdgeDataFd);
		}
		void createTempDegreeFile(){
			// create file
			indegreeFd = open(gfn.tempFileName("_indegree_temp").c_str(),O_CREAT|O_RDWR,0777);
			indegreePrefixFd = open(gfn.tempFileName("_indegreeprefix_temp").c_str(),O_CREAT|O_RDWR,0777);
		}
		void closeTempDegreeFile(){
			close(indegreeFd);
			close(indegreePrefixFd);
		}
		void checkOutdegreeBuffer(int fd){
			if(vertexCount == bufferedDataNumber){
				std::cout << "Flush Buffered Outdegree."<< std::endl;
				writeFile<int>(fd,outdegreeBuffer,outdegreeNbytes);
				outdegreeNbytes = 0;
				vertexCount = 0;
			}
		}
		void writeDegreeFileAndReset(){
			writeFile<int>(indegreeFd,indegreeBuffer,indegreeNbytes);
			writeFile<int>(indegreePrefixFd,indegreePrefixSumBuffer,indegreeNbytes);

			writeFile<int>(totalInDegreeFd,indegreeBuffer,indegreeNbytes);
			writeFile<int>(totalInDegreePrefixSumFd,indegreePrefixSumBuffer,indegreeNbytes);

			indegreePrefixSumBuffer[0] = indegreePrefixSumBuffer[vertexCount];
			indegreeNbytes = 0;
			vertexCount = 0;
		}

		void writeEdgeFileAndReset(){
			writeFile<int>(inFd,inEdgeBuffer,inNbytes);
			writeFile<EdgeDataType>(inEdgeDataFd,edgeDataBuffer,inEdgedataNbytes);
			inNbytes = 0;
			inEdgedataNbytes = 0;
			edgeCount = 0;
		}

		void differFromPreid(){
			if(leftEdgeNumber == 0) ++ indegreeCount;
			while(zeroId < preid){
				checkIndegreeBufferAndFlush();
				indegreePrefixSumBuffer[vertexCount] = indegreeAcc;
				indegreeBuffer[vertexCount] = 0;
				++ vertexCount;
				indegreeNbytes += indegreeByte;
				++ zeroId;
				indegreePrefixSumBuffer[vertexCount] = indegreeAcc;
			}
			checkIndegreeBufferAndFlush();
			indegreeBuffer[vertexCount ++] = indegreeCount;
			indegreeAcc += indegreeCount;
			if(vertexCount <= bufferedDataNumber){
				indegreePrefixSumBuffer[vertexCount] = indegreeAcc;
			}
			indegreeNbytes += indegreeByte;

			if(leftEdgeNumber == 0){
				int temp = preid;
				while(++ temp <= limitVertexNumber){
					checkIndegreeBufferAndFlush();
					indegreePrefixSumBuffer[vertexCount] = indegreeAcc;
					indegreeBuffer[vertexCount] = 0;
					++ vertexCount;
					indegreeNbytes += indegreeByte;
					++ preid;
				}
			}
			indegreeCount = 0;
			zeroId = preid + 1;
		}

		void checkIndegreeBufferAndFlush(){
			if(vertexCount == bufferedDataNumber){
				std::cout << "Flush Indegree data." << std::endl;
				writeFile<int>(indegreeFd,indegreeBuffer,indegreeNbytes);
				writeFile<int>(indegreePrefixFd,indegreePrefixSumBuffer,indegreeNbytes);

				writeFile<int>(totalInDegreeFd,indegreeBuffer,indegreeNbytes);
				writeFile<int>(totalInDegreePrefixSumFd,indegreePrefixSumBuffer,indegreeNbytes);
				vertexCount = 0;
				indegreeNbytes = 0;
			}
		}


		void createSubgraphEdgeAndVertexFiles(){
			closeTempEdgeFile();
			rename(gfn.tempFileName("_edge_temp").c_str(),gfn.subgraphInEdgeFileName(startId,preid).c_str());
			rename(gfn.tempFileName("_edgedata_temp").c_str(),gfn.subgraphInEdgeDataFileName(startId,preid).c_str());

			int vertexdata_fd = open(gfn.subgraphVertexDataFileName(startId,preid).c_str(),O_CREAT|O_RDWR,0777);
			int subgraphVertexNumber = preid - startId + 1;
			long_t datasize  = subgraphVertexNumber * sizeof(VertexDataType);
			VertexDataType *data = (VertexDataType *)calloc(subgraphVertexNumber,sizeof(VertexDataType));
			if(initialVertexDataAlso){
				for(int i = 0; i < subgraphVertexNumber; i ++){
					data[i] = VertexDataType();
				}
			}
			writeFile<VertexDataType>(vertexdata_fd,data,datasize);
			free(data);
			close(vertexdata_fd);

		}
		void createSubgraphIndegreeFiles(){
			closeTempDegreeFile();
			rename(gfn.tempFileName("_indegree_temp").c_str(),gfn.subgraphInDegreeFileName(startId,preid).c_str());
			rename(gfn.tempFileName("_indegreeprefix_temp").c_str(),gfn.subgraphInDegreePrefixSumFileName(startId,preid).c_str());
			indegreeAcc = 0;
			indegreePrefixSumBuffer[0] = 0;
		}

		void parseGraphFile(std::vector<GraphInterval> &newIntervals){
			if((preid != -1 && dst != preid) || leftEdgeNumber == 0 ){
				differFromPreid();
			}
			++ indegreeCount;

			bool fillBuffer = edgeCount == bufferedDataNumber;
			bool fillSubgraph = (totalCountEach >=edgeNumberEach && dst != preid)  || leftEdgeNumber == 0;

			if(fillBuffer ||  fillSubgraph){
				if(leftEdgeNumber == 0){
					if(edgeCount == bufferedDataNumber){
						writeEdgeFileAndReset();
					}
					inEdgeBuffer[edgeCount] = src;
					inNbytes += eachEdgeByte;
					edgeDataBuffer[edgeCount ++] = val;
					inEdgedataNbytes += eachEdgeDataByte;
				}
				writeEdgeFileAndReset();
				if(fillSubgraph){
					// record the subgraph interval
					std::cout<< "Subgraph Interval: "<< startId << "--"<<preid << std::endl;
					newIntervals.push_back(GraphInterval(startId,preid));
					totalCountEach = 0;

					createSubgraphEdgeAndVertexFiles();
					if(leftEdgeNumber > 0){
						createTempEdgeFile();
					}
				}
			}

			inEdgeBuffer[edgeCount] = src;
			inNbytes += eachEdgeByte;
			edgeDataBuffer[edgeCount ++] = val;
			inEdgedataNbytes += eachEdgeDataByte;
			++ totalCountEach;

			bool fillDegreeBuffer = vertexCount == bufferedDataNumber;
			if(fillDegreeBuffer ||  fillSubgraph){
				writeDegreeFileAndReset();
				if(fillSubgraph){
					createSubgraphIndegreeFiles();
					startId = preid + 1;
					if(leftEdgeNumber > 0){
						createTempDegreeFile();
					}
				}
			}
			preid = dst;
		}

		void createTotalVertexDataFile(long_t vertexNumber){
			long_t dataNumber = vertexNumber;
			int totalVertexdataFd = open(gfn.graphVertexDataFileName().c_str(),O_CREAT|O_RDWR,0777);
			long_t totalSize = (dataNumber) * sizeof(VertexDataType);
			VertexDataType *data = (VertexDataType *)calloc(dataNumber,sizeof(VertexDataType));
			if(initialVertexDataAlso){
				for(long_t i = 0; i < dataNumber; i ++){
					data[i] = VertexDataType();
				}
			}
			writeFile<VertexDataType>(totalVertexdataFd,data,totalSize);
			free(data);
			close(totalVertexdataFd);
		}

		void convertBipartiteGraphInDstOrder(ParseCmd &pCmd,MemoryAllocResult& mar,bool _initialVertexDataAlso = true){
			if(hasConverted()){
				// reload memory allocation infomation.
				FILE *mfd = fopen(gfn.memoryAllocInfoName().c_str(),"rb");
				if(fread(&mar,sizeof(mar),1,mfd) == 1)
					fclose(mfd);
				else{
					std::cout << "Error: Can not open the file: " << gfn.memoryAllocInfoName() << std::endl;
					assert(false);
				}
				return;
			}
			// memory allocation for vertex data, graph structure and so on. Depending on total memory,
			// graph's #vertex and #edge. Also return the appropriate subgraph number.(may not equal the number in command)

			memoryAlloc<VertexDataType,EdgeDataType>(mar,pCmd.getConfigInt("totalMemory"),fileName,pCmd.getConfigInt("nsub"),false);
			initialVertexDataAlso = _initialVertexDataAlso;
			srand((int)time(NULL));
			int subNum = mar.subgraphNumber;
			std::ifstream inf(fileName.c_str());
			if(!inf.good()){
				std::cout << "Error: can not open the file." << std::endl;
				assert(false);
			}

			long_t linenum = 0;
			long_t rowNumber = 0;// row number
			long_t columnNumber = 0; // column number:start from the max row number id
			vertex_t trans = 0;

			createTempEdgeFile();
			createTempDegreeFile();

			totalInDegreeFd = open(gfn.graphInDegreeFileName().c_str(),O_CREAT|O_RDWR,0777);
			totalInDegreePrefixSumFd = open(gfn.graphInDegreePrefixSumFileName().c_str(),O_CREAT|O_RDWR,0777);

			char line[1024] = {0};
			while(inf.getline(line,1024)){
				if(line[0] == '#' || line[0] == '%'){
					// comment
					continue;
				}
				linenum++;
				if(linenum % 10000000 == 0){
					std::cout<< "Processed Edge Number:" << linenum << std::endl;
				}
				char splitChar[] = "\t, ";
				char * data;
				data = strtok(line, splitChar);
				src = atoi(data);

				data = strtok(NULL, splitChar);
				dst = atoi(data);

				data = strtok(NULL, splitChar);

				if (data != NULL) {
					val = atof(data);;
				}
				if(linenum == 1){
					// assume the first line of bipartite graph: #u, #p, #edge
					rowNumber = src;
					columnNumber = dst;
					edgeNumberEach = val / subNum;// possible number of edge in each subgraph
					startId = rowNumber+1;
					zeroId = startId;
					leftEdgeNumber = val;
					totalEdgeNumber = val;
					trans = src;
					continue;
				}
				dst = trans + dst;
				-- leftEdgeNumber;
				
				parseGraphFile(subgraphIntervals);
			}
			inf.close();

			close(totalInDegreeFd);
			close(totalInDegreePrefixSumFd);

			// graph interval info
			std::string fname = gfn.graphSubInterval();
			FILE * f = fopen(fname.c_str(), "w");
			assert(f != NULL);
			fprintf(f, "%lu\n", rowNumber+1);// start vertexId
			for(int i=0; i<(int)subgraphIntervals.size(); i++) {
			   fprintf(f, "%d\n", subgraphIntervals[i].second);
			}
			fclose(f);

			// 7,total vertex data file
			createTotalVertexDataFile(rowNumber + columnNumber+1);
			releaseBuffers();

			//Save the memory allocation result. And read it directly.
			mar.vertexNumber = rowNumber + columnNumber +1;
			mar.edgeNumber = totalEdgeNumber;
			FILE *mfd = fopen(gfn.memoryAllocInfoName().c_str(),"wb");
			fwrite(&mar,sizeof(mar),1,mfd);
			fclose(mfd);
		}
		// parameters:1,cmd parser; 2,MemoryAllocResult object: memory allocation info; 3,whether initial vertex data or not
		void convertBipartiteInDstOrderAsUndirecedGraph(ParseCmd &pCmd,MemoryAllocResult& mar,bool _initialVertexDataAlso=false){
			if(hasConverted()){
				// load memory configuration info from file
				FILE *mfd = fopen(gfn.memoryAllocInfoName().c_str(),"rb");
				if(fread(&mar,sizeof(mar),1,mfd) == 1)
					fclose(mfd);
				else{
					std::cout << "Error: Can not open the file: " << gfn.memoryAllocInfoName() << std::endl;
					assert(false);
				}
				
				return;
			}
			std::cout << "Get Memory Allocation Info for Preprocess." << std::endl;
			memoryAlloc<VertexDataType,EdgeDataType>(mar,pCmd.getConfigInt("totalMemory"),fileName,pCmd.getConfigInt("nsub"),false);
			initialVertexDataAlso = _initialVertexDataAlso;
			srand((int)time(NULL));
			int subNum = mar.subgraphNumber;
			std::ifstream inf(fileName.c_str());
			if(!inf.good()){
				std::cout << "Error: can not open the file." << std::endl;
				assert(false);
			}

			long_t linenum = 0;
			long_t rowNumber = 0;// row number
			long_t columnNumber = 0; // column number:start from the max row number id
			vertex_t trans = 0;

			createTempEdgeFile();
			createTempDegreeFile();

			// the original indegree and indegree prefix sum file. they will be merged with the reversed files.
			std::stringstream so;so << gfn.getFileName() << "_original_indegree";
			std::stringstream soPre;soPre << gfn.getFileName() << "_original_indegree_pre";

			totalInDegreeFd = open(so.str().c_str(),O_CREAT|O_RDWR,0777);
			totalInDegreePrefixSumFd = open(soPre.str().c_str(),O_CREAT|O_RDWR,0777);

			int bufEdgeNumber = 10 * 1024 * 1024;// for reversed buffer
			int reverseEdgeSize = sizeof(EdgeWithValue<EdgeDataType>);
			EdgeWithValue<EdgeDataType> *reverseBuffer = (EdgeWithValue<EdgeDataType> *)malloc(bufEdgeNumber * reverseEdgeSize);
			int reverseEdgeCount = 0;
			int reverseBytes = 0;
			int fileBlock = 0;// the number of temporary file

			char line[1024] = {0};
			while(inf.getline(line,1024)){
				if(line[0] == '#' || line[0] == '%'){
					continue;
				}
				linenum++;
				if(linenum % 10000000 == 0){
					std::cout<< "Processing Edge Number:" << linenum << std::endl;
				}
				char splitChar[] = "\t, ";
				char * data;
				data = strtok(line, splitChar);
				src = atoi(data);

				data = strtok(NULL, splitChar);
				dst = atoi(data);

				data = strtok(NULL, splitChar);

				if (data != NULL) {
					val = atof(data);;
				}
				if(linenum == 1){
					// assume the first line of bipartite graph: #u, #p, #edge
					rowNumber = src;
					limitVertexNumber = rowNumber;
					columnNumber = dst;
					totalEdgeNumber = val;
					edgeNumberEach = val / subNum;// possible number of edge in each subgraph
					startId = rowNumber+1;
					zeroId = startId;
					leftEdgeNumber = val;
					trans = src;

					std::cout << "Graph Info: EdgeNumber=" << totalEdgeNumber<< ", VertexNumber=" << (rowNumber+columnNumber)<< std::endl;
					continue;
				}
				dst = trans + dst;
				// create reverse edges
				reverseBuffer[reverseEdgeCount] = EdgeWithValue<EdgeDataType>(dst,src,val);
				reverseBytes += reverseEdgeSize;
				if(++ reverseEdgeCount == bufEdgeNumber){
					std::cout << "Write temporary file:" << fileBlock << std::endl;
					int reverseFd = open(gfn.binaryTempFileName(fileBlock ++).c_str(),O_CREAT|O_RDWR,0777);
					// sort the reverseBuffer as dst
					quickSort<EdgeWithValue<EdgeDataType> >(reverseBuffer,0,reverseBytes / reverseEdgeSize - 1);
					writeFile<EdgeWithValue<EdgeDataType> >(reverseFd,reverseBuffer,reverseBytes);
					close(reverseFd);

					reverseBytes = 0;
					reverseEdgeCount = 0;
				}

				-- leftEdgeNumber;
				parseGraphFile(subgraphIntervals);
			}
			inf.close();

			if(reverseBytes > 0){
				int reverseFd = open(gfn.binaryTempFileName(fileBlock ++).c_str(),O_CREAT|O_RDWR,0777);
				quickSort<EdgeWithValue<EdgeDataType> >(reverseBuffer,0,reverseBytes / reverseEdgeSize - 1);
				writeFile<EdgeWithValue<EdgeDataType> >(reverseFd,reverseBuffer,reverseBytes);
				close(reverseFd);
			}

			free(reverseBuffer);

			close(totalInDegreeFd);
			close(totalInDegreePrefixSumFd);

			std::cout<< "Start to Sort Graph..." << std::endl;
			// sort graph file
			SortFile<EdgeWithValue<EdgeDataType> >sf(fileBlock,gfn.getFileName());
			sf.sort();
			/**
			 * create reversed graph structure by using the sorted file.
			 * parameters:1,#subgraph;  2,total edge number in graph;  3,the original subgraph intervals;
			 * 4,total indegree file name;  5,total indegree prefix sum file name
			 */
			std::cout<< "Start to Process Sorted Graph..." << std::endl;
			createSubgraphFromBinaryFile(subNum,totalEdgeNumber,so.str(),soPre.str());
			remove(gfn.sortedFileName().c_str());

			mar.vertexNumber = rowNumber + columnNumber + 1;
			mar.subgraphNumber = subNum * 2;
			// 7,total vertex data file
			createTotalVertexDataFile(rowNumber + columnNumber+1);
			releaseBuffers();

			FILE *mfd = fopen(gfn.memoryAllocInfoName().c_str(),"wb");
			fwrite(&mar,sizeof(mar),1,mfd);
			fclose(mfd);
		}


		void createSubgraphFromBinaryFile(int subNum,long lineNumber,
				std::string originalTotalIndegree, std::string originalTotalInPreSum,VertexDataType initialVertexData = VertexDataType()){
			FILE *inf = fopen(gfn.sortedFileName().c_str(),"rb");
			if(inf == NULL){
				std::cout << "Error: Can not open the sorted graph file." << std::endl;
				assert(false);
			}

			long_t linenum = 0;
			std::vector<GraphInterval> newIntervals;// record subgraph interval

			resetBufferBytes();
			createTempEdgeFile();
			createTempDegreeFile();
			totalInDegreeFd = open(gfn.graphInDegreeFileName().c_str(),O_CREAT|O_RDWR,0777);
			totalInDegreePrefixSumFd = open(gfn.graphInDegreePrefixSumFileName().c_str(),O_CREAT|O_RDWR,0777);

			edgeNumberEach = lineNumber / subNum;// possible number of edge in each subgraph
			leftEdgeNumber = lineNumber;
			EdgeWithValue<EdgeDataType> edge;

			while(fread(&edge,sizeof(edge),1,inf) == 1){
				src = edge.src;
				dst = edge.dst;
				val = edge.edgeValue;
				++ linenum;
				if(linenum % 10000000 == 0)
					std::cout << "Processed edge number:" << linenum << std::endl;  
				-- leftEdgeNumber;
				parseGraphFile(newIntervals);
			}
			fclose(inf);

			if(originalTotalIndegree != "" && originalTotalInPreSum != ""){
				copyFile(originalTotalIndegree,totalInDegreeFd);
				copyFile(originalTotalInPreSum,totalInDegreePrefixSumFd);
			}

			close(totalInDegreeFd);
			close(totalInDegreePrefixSumFd);

			// graph interval info
			std::string fname = gfn.graphSubInterval();
			FILE * f = fopen(fname.c_str(), "w");
			assert(f != NULL);
			fprintf(f, "%u\n", 0);// start vertexId
			for(int i=0; i<(int)newIntervals.size(); i++) {
			   fprintf(f, "%u\n", newIntervals[i].second);
			}
			for(int i=0; i<(int)subgraphIntervals.size(); i++) {
			   fprintf(f, "%u\n", subgraphIntervals[i].second);
			}
			fclose(f);
			releaseBuffers();
		}

		void copyFile(std::string srcFile, int dstFd){
			int bufferSize = 2 * 1024 * 1024;
			char *copyBuf = (char*)malloc(bufferSize);
			long_t fileSize = getFileSize(srcFile.c_str());
			int writeNumber = fileSize / bufferSize + 1;

			int srcFd = open(srcFile.c_str(),O_RDONLY);
			for(int i = 0; i < writeNumber; i ++){
				size_t copySize = bufferSize;
				if(i == writeNumber - 1){
					copySize = fileSize - i * bufferSize;
				}

				readFile<char>(srcFd,copyBuf,copySize,i * bufferSize);
				writeFile<char>(dstFd,copyBuf,copySize);
			}
			free(copyBuf);
			close(srcFd);
			remove(srcFile.c_str());
		}

		// Subgraph interval file is necessary.
		inline bool hasConverted(){
			int tempFd = open(gfn.graphSubInterval().c_str(),O_RDONLY);
			bool b = (tempFd >= 0)?true:false;
			close(tempFd);
			if(b == true){
				std::cout<< "Graph has been processed, and load process info from disk." << std::endl;
			}
			return b;
		}


		// the graph file is in source order
		void convertGraphInSrcOrder(ParseCmd &pCmd,MemoryAllocResult& mar,bool _initialVertexDataAlso,VertexDataType initialData = VertexDataType()){
			if(hasConverted()){
				// load memory configuration info from file
				FILE *mfd = fopen(gfn.memoryAllocInfoName().c_str(),"rb");
				if(fread(&mar,sizeof(mar),1,mfd) == 1)
					fclose(mfd);
				else{
					std::cout << "Error: Can not open the file: " << gfn.memoryAllocInfoName() << std::endl;
					assert(false);
				}
				return;
			}
			memoryAlloc<VertexDataType,EdgeDataType>(mar,pCmd.getConfigInt("totalMemory"),fileName,pCmd.getConfigInt("nsub"));
			initialVertexDataAlso = _initialVertexDataAlso;
			srand((int)time(NULL));
			int subNum = mar.subgraphNumber;
			std::ifstream inf(fileName.c_str());
			if(!inf.good()){
				std::cout << "Error: can not open the file." << std::endl;
				assert(false);
			}

			long totalEdgeNumber = 0L;
			vertex_t preid = -1;// previous vertex id
			long outdegreeCount = 0;// the out degree of a vertex
			vertexCount = 0;
			// only create the out degree file.
			int totalOutdegreeFd = 0;// only the outdegree data
			totalOutdegreeFd = open(gfn.graphOutDegreeFileName().c_str(),O_CREAT|O_RDWR,0777);

			size_t bufEdgeNumber = 20 * 1024 * 1024;
			size_t reverseEdgeSize = sizeof(EdgeWithValue<EdgeDataType>);
			EdgeWithValue<EdgeDataType> *reverseBuffer = (EdgeWithValue<EdgeDataType> *)malloc(bufEdgeNumber * reverseEdgeSize);
			int reverseEdgeCount = 0;
			size_t reverseBytes = 0;
			int fileBlock = 0;// the number of temporary file

			char line[1024] = {0};
			while(inf.getline(line,1024)){
				if(line[0] == '#' || line[0] == '%'){
					continue;
				}
				++ totalEdgeNumber;
				if(totalEdgeNumber % 10000000 == 0){
					std::cout << "Processed Edge Number:" << totalEdgeNumber << std::endl;
				}
				char splitChar[] = "\t, ";
				char * data;
				data = strtok(line, splitChar);
				int src = atoi(data);

				data = strtok(NULL, splitChar);
				int dst = atoi(data);

				data = strtok(NULL, splitChar);

				if (data != NULL) {
					val = atof(data);;
				}
				// create reverse edges
				reverseBuffer[reverseEdgeCount] = EdgeWithValue<EdgeDataType>(src,dst,val);
				reverseBytes += reverseEdgeSize;
				if(++ reverseEdgeCount == (int)bufEdgeNumber){
					std::cout << "Write temporary file" << fileBlock << std::endl;
					int reverseFd = open(gfn.binaryTempFileName(fileBlock ++).c_str(),O_CREAT|O_RDWR,0777);
					// sort the reverseBuffer as dst
					quickSort<EdgeWithValue<EdgeDataType> >(reverseBuffer,0,reverseBytes / reverseEdgeSize - 1);
					writeFile<EdgeWithValue<EdgeDataType> >(reverseFd,reverseBuffer,reverseBytes);
					close(reverseFd);
					reverseBytes = 0;
					reverseEdgeCount = 0;
				}


				// compared with source vertex
				if((preid != -1 && src != preid)){
					while(startId < preid){
						checkOutdegreeBuffer(totalOutdegreeFd);
						outdegreeBuffer[vertexCount ++] = 0;
						outdegreeNbytes += indegreeByte;
						++ startId;
					}
					checkOutdegreeBuffer(totalOutdegreeFd);
					outdegreeBuffer[vertexCount ++] = outdegreeCount;
					outdegreeNbytes += indegreeByte;
					outdegreeCount = 0;
					startId = preid + 1;
				}
				++ outdegreeCount;

				preid = src;
			}
			while(startId < preid){
				checkOutdegreeBuffer(totalOutdegreeFd);
				outdegreeBuffer[vertexCount ++] = 0;
				outdegreeNbytes += indegreeByte;
				++ startId;
			}
			checkOutdegreeBuffer(totalOutdegreeFd);
			outdegreeBuffer[vertexCount ++] = outdegreeCount;
			outdegreeNbytes += indegreeByte;
			writeFile<int>(totalOutdegreeFd,outdegreeBuffer,outdegreeNbytes);

			close(totalOutdegreeFd);
			
			totalVertexNumber = getFileSize(gfn.graphOutDegreeFileName().c_str())/indegreeByte;
			std::cout << "Finish Process Original Graph File: Edge Number = " << totalEdgeNumber << " Vertex Number = " << totalVertexNumber << std::endl;
			limitVertexNumber = totalVertexNumber - 1;
			free(outdegreeBuffer);outdegreeBuffer = NULL;
			inf.close();

			if(reverseBytes > 0){
				int reverseFd = open(gfn.binaryTempFileName(fileBlock ++).c_str(),O_CREAT|O_RDWR,0777);
				quickSort<EdgeWithValue<EdgeDataType> >(reverseBuffer,0,reverseBytes / reverseEdgeSize - 1);
				writeFile<EdgeWithValue<EdgeDataType> >(reverseFd,reverseBuffer,reverseBytes);
				close(reverseFd);
			}

			free(reverseBuffer);
			

			std::cout << "Start to sort graph file." << std::endl;
			SortFile<EdgeWithValue<EdgeDataType> >sf(fileBlock,gfn.getFileName());
			sf.sort();
			/**
			 * create reversed graph structure by using the sorted file.
			 * parameters:1,#subgraph;  2,total edge number in graph;  3,the original subgraph intervals;
			 * 4,total indegree file name;  5,total indegree prefix sum file name
			 */
			createSubgraphFromBinaryFile(subNum,totalEdgeNumber,"","",initialData);
			// 7,total vertex data file
			createTotalVertexDataFile(totalVertexNumber);
			releaseBuffers();

			mar.vertexNumber = totalVertexNumber;
			mar.edgeNumber = totalEdgeNumber;
			FILE *mfd = fopen(gfn.memoryAllocInfoName().c_str(),"wb");
			fwrite(&mar,sizeof(mar),1,mfd);
			fclose(mfd);
		}
	};
}

#endif /* PREPROCESS_HPP_ */
