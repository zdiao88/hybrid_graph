/**
 * @author zhangdiao
 * @brief 
 * record time
 */

#ifndef TIME_RECORD_HPP_
#define TIME_RECORD_HPP_

#include <sys/time.h>
#include <map>
#include <string>
#include <iostream>


namespace graph{

	class TimeInfo{
	public:
		timeval startTime;
		double totalTime;
	public:
		TimeInfo(){}
		inline void start(){
			gettimeofday(&startTime, NULL);
		}

		inline void stop(){
			timeval end;
			gettimeofday(&end, NULL);
			double lastTime = end.tv_sec - startTime.tv_sec + ((double)(end.tv_usec - startTime.tv_usec)) / 1.0E6;
			totalTime += lastTime;
		}
	};

	class TimeRecord{
	private:
		std::map<std::string, TimeInfo> times;

	public:
		TimeRecord(){}

		inline void clear(){
			times.clear();
		}
		inline void startTime(std::string recordItem){
			if(times.count(recordItem) == 0){
				times[recordItem] = TimeInfo();
			}
			times[recordItem].start();
		}

		inline void stopTime(std::string recordItem){
			times[recordItem].stop();
		}

		inline void showTimes(){
			std::cout << "=======Time Report=======" << std::endl;
			std::map<std::string, TimeInfo>::iterator iter;
			for(iter = times.begin(); iter != times.end(); iter ++){
				std::string key = iter ->first;
				double value = (iter ->second).totalTime;
				std::cout << key << ": " << value << std::endl;
			}
			std::cout << "=========================" << std::endl;
		}
	};
}

#endif /* TIME_RECORD_HPP_ */
