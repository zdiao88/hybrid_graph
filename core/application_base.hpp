/**
 * @author zhangdiao
 * @brief 
 * Application should inherit ApplicationBase.
 */

#ifndef APPLICATON_BASE_HPP_
#define APPLICATON_BASE_HPP_

#include "graph_type.hpp"

namespace graph{

	struct GraphCoreInfo{
		bool sameInitialVertexData;
		GraphCoreInfo(){}
	};

	template <typename VertexDataType, typename EdgeDataType>
	class ApplicationBase{
	public:
		virtual ~ApplicationBase(){}
		virtual void updateVertexModel(GraphVertex<VertexDataType,EdgeDataType> &vertex,GraphCoreInfo& info) = 0;
		virtual VertexDataType initVertexData(GraphCoreInfo &info) = 0;
	};
}
#endif /* APPLICATON_BASE_HPP_ */
