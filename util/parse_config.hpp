/**
 * @author zhangdiao
 * @brief 
 * ParseCmd: parse command line options.
 */

#ifndef PARSE_CONFIG_HPP_
#define PARSE_CONFIG_HPP_

#include <iostream>
#include <string>
#include <map>
#include <cassert>

namespace graph{
	class ParseCmd{
	private:
		int argc;
		char **argv;
		std::map<std::string,std::string> configuration;
	public:
		void getConfigFromCmd();
		void getConfigFromFile();
		inline std::string trim(std::string str);

	public:
		ParseCmd(int _argc, char **_argv):argc(_argc),argv(_argv){
			getConfigFromFile();
			getConfigFromCmd();
		}
		std::string getConfigString(const char *option);
		int getConfigInt(const char *option);
		inline void showAllConfig(){
			std::map<std::string,std::string>::iterator iter;
			for(iter = configuration.begin(); iter != configuration.end(); iter ++){
				std::cout << iter -> first << "=" << iter -> second << std::endl;
			}
		}
	};
}
#endif /* PARSE_CONFIG_HPP_ */
