/**
 * @author zhangdiao
 * PageRank
 */

#include "graph_include.hpp"

using namespace graph;

typedef my_vertex_type VertexDataType;
typedef my_edge_type EdgeDataType;


class MyGraphApp : public ApplicationBase<VertexDataType,EdgeDataType> {

	VertexDataType initVertexData(GraphCoreInfo &info) {
		return initialData;
	}

	void updateVertexModel(GraphVertex<VertexDataType,EdgeDataType> &vertex, GraphCoreInfo &info) {

		int inc = vertex.getInDegree();
		for (int i = 0; i < inc; i ++) {
			// Do something
		}

		vertex.setVertexData(data);
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

    	// e.g. PageRank: return *src / outc;
}

int main(int argc, char **argv) {
	
	ParseCmd pCmd(argc,argv); // parse command and load run.conf
	MemoryAllocResult mar; // the memory allocation result
	std::string fileName = pCmd.getConfigString("file");

	GraphConversion<VertexDataType,EdgeDataType> gc(fileName);
	//bool parameters: init vertex data or not
	gc.convertSrcOrderGraph(pCmd, mar, true, true);
	// Application
	PageRankApp app;
	int niters = pCmd.getConfigInt("niters");
	GraphCore<VertexDataType,EdgeDataType> core(fileName,mar);
	core.setInEdgeRelation(relation);

	// optimization
	// core.setReadEdgeData(false);
	// core.setNeedCacheForEdgeData(false);

	core.run(app,niters);
	return 0;
}


