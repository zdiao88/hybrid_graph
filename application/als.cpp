/**
 * @author zhangdiao
 * @brief 
 * ALS
 */

#include "graph_include.hpp"
#include <cstdio>
#include <ctime>
#include <vector>
#include <Eigen/Dense>

#define DIMENSION 20

using Eigen::VectorXd;
using Eigen::MatrixXd;

using namespace graph;
struct Feature{
	double d[DIMENSION];
	Feature(){
		for(int k=0; k < DIMENSION; k++) d[k] =  rand()%5*0.115;
	}
};

std::vector<Feature> vertexDatas;
double LAMBDA = 0.065;

typedef Feature VertexDataType;
typedef double EdgeDataType;


class AlsApp : public ApplicationBase<VertexDataType,EdgeDataType>{

	VertexDataType initVertexData(GraphCoreInfo &info){
		return VertexDataType();
	}

	void updateVertexModel(GraphVertex<VertexDataType,EdgeDataType> &vertex, GraphCoreInfo &info){
		MatrixXd XtX(DIMENSION,DIMENSION);
		XtX.setZero();
		VectorXd Xty(DIMENSION);
		Xty.setZero();

		// lower triangular matrix.
		int inc = vertex.getInDegree();
		for(int i = 0; i < inc; i ++){ 
			double observation = (vertex.getInEdgeData(i))->edgedata;
			Feature &src = vertexDatas[(vertex.getInEdge(i))->srcId];
			for(int i=0; i < DIMENSION; i ++){
				Xty(i) += src.d[i] * observation;
				for(int j = i; j < DIMENSION; j ++){
					XtX(j,i) += src.d[i] * src.d[j];
				}
			}
		}

		// make symmetrize
		for(int i = 0; i < DIMENSION; i ++){
			for(int j = i+1; j < DIMENSION; j ++){
				XtX(i,j) = XtX(j,i);
			}
		}

		// diagonal
		for(int i = 0; i < DIMENSION; i ++){
			XtX(i,i) += LAMBDA * vertex.getInDegree();
		}
		VectorXd veclatent = XtX.ldlt().solve(Xty);
		Feature newlatent;
        for(int i=0; i < DIMENSION; i++) newlatent.d[i] = veclatent[i];
		vertex.setVertexData(newlatent);
		vertexDatas[vertex.getVertexId()] = newlatent;
	}
};

EdgeDataType relation(VertexDataType *src,int outc,VertexDataType *dst,EdgeDataType *originalEdgeData){
    	return *originalEdgeData;
}

int main(int argc, char **argv){
	ParseCmd pCmd(argc,argv);// parse command and load run.conf
	MemoryAllocResult mar;// the memory allocation result
	std::string fileName = pCmd.getConfigString("file");

	GraphConversion<VertexDataType,EdgeDataType> gc(fileName);
	//convert bipartite graph for matrix factorization.
	//bool parameters: init vertex data or not
	gc.convertDstOrderBipartiteGraphAsUndirecedGraph(pCmd, mar, false, false);
	srand(time(NULL));
	// put all vertex data in memory
	vertexDatas.resize(mar.vertexNumber);
	for(int i = 0;i < mar.vertexNumber; i ++){
		vertexDatas.push_back(Feature());
	}

	// Application
	AlsApp app;
	int niters = pCmd.getConfigInt("niters");
	GraphCore<VertexDataType,EdgeDataType> core(fileName,mar);
	core.initialVertexFromFile();
	core.setInEdgeRelation(relation);
	core.setNeedCacheForEdgeData(false);
	core.setUseInDegreeOnly();
	core.setUpdateVertexDataWithEdgeData(true);
	core.run(app,niters);
/*
int row = 5,column = 4;
        for(int i = 1; i <= row; i ++){
		for(int j = row+1; j <= column+row; j ++){
			float temp = 0.0f;
			for(int k = 0; k < DIMENSION; k ++){
				temp += vertexDatas[i].d[k] * vertexDatas[j].d[k];
			}
			std::cout << temp << ",";
		}
		std::cout << std::endl;
	}
*/
	// before return you can write the vertexDatas into file.
	return 0;
}



