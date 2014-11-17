/**
 * @author zhangdiao
 * @brief 
 * ParseCmd: parse command line options.
 */

#include <fstream>
#include "parse_config.hpp"
#include <cstdlib>
#include <cstring>

namespace graph{
	std::string ParseCmd::getConfigString(const char *option){
		std::string key = std::string(option);
		if(configuration.count(key) == 0){
			std::cout << "Error: Has no option named:" << key << std::endl;
			assert(false);
		}
		return configuration[key];
	}
	int ParseCmd::getConfigInt(const char *option){
		std::string key = std::string(option);
		if(configuration.count(key) == 0){
			std::cout << "Error: Has no option named:" << key << std::endl;
			assert(false);
		}
		return atoi(configuration[key].c_str());
	}

	// command line's option can override run.conf
	void ParseCmd::getConfigFromCmd(){
		if((argc - 1) % 2 != 0){
			std::cout << "Error: Command is wrong." << std::endl;
			assert(false);
		}
		for(int i = 1; i < argc; i += 2){
			std::string key = std::string(argv[i]);
			std::string value = std::string(argv[i+1]);
			configuration[key] = value;
		}
	}

	// load configuration file:run.conf，e.g.totalMemory = 5000
	void ParseCmd::getConfigFromFile(){
		std::ifstream inf("run.conf");
		if(!inf.good()){
			std::cout << "Error: can not load the configuration file." << std::endl;
			assert(false);
		}
		char line[1024] = {0};
		std::string key("");
		std::string value("");
		while(inf.getline(line,1024)){
			if(line[0] == '#' || line[0] == '%'){
				continue;
			}
			// split string:e.g. totalMemory = 4000
			char splitChar[]="=";
			char *tempKey = NULL;
			char *tempValue = NULL;
			tempKey = strtok(line,splitChar);
			tempValue = strtok(NULL,splitChar);

			if(tempKey != NULL && tempValue != NULL){
				std::string key = trim(std::string(tempKey));
				std::string value = trim(std::string(tempValue));
				configuration[key] = value;
			}
		}
	}

	std::string ParseCmd::trim(std::string str){
		std::string trimChars = " \n\r\t";
		std::string::size_type position = str.find_first_not_of(trimChars);
		if(position != std::string::npos)
			str.erase(0,position);
		position = str.find_last_not_of(trimChars);
		if(position != std::string::npos)
			str.erase(position+1);
		return str;
	}
}
