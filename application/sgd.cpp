/**
 * @author zhangdiao
 * @brief 
 * SGD
 */

#include "graph_include.hpp"
#include <ctime>
#include <cstdio>

#define ALPHA 0.002
#define BETA  0.02
#define DIMENSION 20
#define MINVALUE 1
#define MAXVALUE 5

using namespace graph;
struct Feature{
	float d[DIMENSION];
	Feature(){
		srand(time(NULL));
		for(int k=0; k < DIMENSION; k++) d[k] =  rand()%5*0.115;
	}
};

typedef Feature VertexDataType;
typedef float EdgeDataType;


class SgdApp : public ApplicationBase<VertexDataType,EdgeDataType>{

	VertexDataType initVertexData(GraphCoreInfo &info){
		return VertexDataType();
	}

	void updateVertexModel(GraphVertex<VertexDataType,EdgeDataType> &vertex, GraphCoreInfo &info){

	}
};

EdgeDataType relation(VertexDataType *src,int outc,VertexDataType *dst,EdgeDataType *originalEdgeData){
    	float temp_new_edgedata = 0.0f;
    	for(int i = 0; i < DIMENSION; i ++){
    		temp_new_edgedata += src->d[i] * dst->d[i];
    	}
	if(DIMENSION > 5)
	{
		if(temp_new_edgedata > MAXVALUE)temp_new_edgedata = MAXVALUE;
		if(temp_new_edgedata < MINVALUE)temp_new_edgedata = MINVALUE;
	}

    	float error = *originalEdgeData - temp_new_edgedata;
	
		for(int i = 0; i < DIMENSION; i ++){
			src->d[i] += ALPHA *(2*error*dst->d[i] - BETA * src->d[i]);
			dst->d[i] += ALPHA *(2*error*src->d[i] - BETA * dst->d[i]);
		}
    	return *originalEdgeData;
}

int main(int argc, char **argv){
	ParseCmd pCmd(argc,argv);// parse command and load run.conf
	MemoryAllocResult mar;// the memory allocation result
	std::string fileName = pCmd.getConfigString("file");

	GraphConversion<VertexDataType,EdgeDataType> gc(fileName);
	//convert bipartite graph for matrix factorization.
	//bool parameters: init vertex data or not
	gc.convertBipartiteGraphInDstOrder(pCmd,mar,true);
	// Application
	SgdApp app;
	int niters = pCmd.getConfigInt("niters");
	GraphCore<VertexDataType,EdgeDataType> core(fileName,mar);
	core.setInEdgeRelation(relation);
	core.initialVertexFromFile();
	core.setNeedCacheForEdgeData(false);
	core.setUseInDegreeOnly();
	core.setNeedExcuteUpdateForVertex(false);
	core.setUpdateVertexDataWithEdgeData(true);

	core.run(app,niters);

	return 0;
}
