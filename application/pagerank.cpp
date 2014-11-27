/**
 * @author zhangdiao
 * PageRank
 */

#include "graph_include.hpp"
#include "../util/result_process.hpp" 

#define INITIAL_VERTEX_DATA 0.15
using namespace graph;

// define the vertex data type
struct PageRank {
	float prData;
	PageRank():prData(INITIAL_VERTEX_DATA) {}
	PageRank(float _prData):prData(_prData) {}
	float operator/ (int outc){
		return prData / outc;
	}
};

typedef PageRank VertexDataType;
typedef float EdgeDataType;


class PageRankApp : public ApplicationBase<VertexDataType,EdgeDataType> {

	VertexDataType initVertexData(GraphCoreInfo &info) {
		return INITIAL_VERTEX_DATA;
	}

	void updateVertexModel(GraphVertex<VertexDataType,EdgeDataType> &vertex, GraphCoreInfo &info) {
		float sum = 0;
		int inc = vertex.getInDegree();
		for (int i = 0; i < inc; i ++) {
			float val = (vertex.getInEdgeData(i)) -> edgedata;
			sum += val;
		}

		float pageRank = INITIAL_VERTEX_DATA + (1 - INITIAL_VERTEX_DATA) * sum;
		vertex.setVertexData(PageRank(pageRank));
	}
};

/* define the relation about a edge: 
	(1) the source vertex data;
	(2) outdegree of source vertex;
	(3) the destination vertex data;
	(4) edge data
*/
EdgeDataType relation(VertexDataType *src, int outc, VertexDataType *dst, 
	EdgeDataType *originalEdgeData) {
    	return *src / outc;
}

int main(int argc, char **argv) {
	
	ParseCmd pCmd(argc,argv);// parse command and load run.conf
	MemoryAllocResult mar;// the memory allocation result
	std::string fileName = pCmd.getConfigString("file");

	GraphConversion<VertexDataType,EdgeDataType> gc(fileName);
	//bool parameters: init vertex data or not
	gc.convertSrcOrderGraph(pCmd, mar, true, true);
	// Application
	PageRankApp app;
	int niters = pCmd.getConfigInt("niters");
	GraphCore<VertexDataType,EdgeDataType> core(fileName,mar);
	core.setInEdgeRelation(relation);
	core.initialVertexFromFile();
	core.setReadEdgeData(false);
	core.setNeedCacheForEdgeData(false);
	core.setUnifiedData(PageRank(1.0f));
	core.run(app,niters);

	int k = pCmd.getConfigInt("topK");
	std::vector<VertexInfo> topK = getTopK<float>(gc.getGFN().graphVertexDataFileName(),k);
	int size = topK.size();
	std::cout << "The top " << size << " PageRank:"  << std::endl;
	
	for(int i = 0; i < size; i ++){
		std::cout << i+1 << ":  " << topK[i].vertexId << ",   " << topK[i].vertexData << std::endl;
	}

	return 0;
}




