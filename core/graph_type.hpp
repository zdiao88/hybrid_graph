/**
 * @author zhangdiao
 * @brief 
 * Define vertex, edge and edge data.
 */

#ifndef GRAPH_TYPE_HPP_
#define GRAPH_TYPE_HPP_

#include <cassert>

namespace graph{

	struct GraphInEdge{
		int srcId;
		int outc;
		GraphInEdge(){}
		GraphInEdge(int _srcId, int _outc):srcId(_srcId),outc(_outc){}
	};

	template <typename EdgeDataType>
	struct GraphEdgeData{
		EdgeDataType edgedata;
	};

	template <typename VertexDataType, typename EdgeDataType>
	class GraphVertex{
	private:
		int vertexId;
		VertexDataType *vertexData;
		int inDegree;

		GraphInEdge *inEdge;
		GraphEdgeData<EdgeDataType> *inEdgeData;

	public:
		GraphVertex(){}
		GraphVertex(int _vertexId, VertexDataType *_vertexData,
				int _inDegree, GraphInEdge *_inEdge, GraphEdgeData<EdgeDataType> *_inEdgeData):
					vertexId(_vertexId),vertexData(_vertexData),inDegree(_inDegree),inEdge(_inEdge),
					inEdgeData(_inEdgeData){
		}

		inline int getVertexId(){return vertexId;}
		inline VertexDataType *getVertexData(){return vertexData;}
		inline void setVertexData(VertexDataType _vertexData){ *vertexData = _vertexData;}


		inline int getInDegree(){return inDegree;}
		inline GraphInEdge *getInEdge(int i){
			assert(i >= 0 && i < inDegree);
			return inEdge + i;
		}

		inline GraphEdgeData<EdgeDataType> *getInEdgeData(int i){
			assert(i >= 0 && i < inDegree);
			return inEdgeData + i;
		}
	};
}

#endif /* GRAPH_TYPE_HPP_ */
