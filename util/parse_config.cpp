/**
 * @author zhangdiao
 * ParseCmd: parse command line options.
 */

#include <fstream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <unistd.h> // chdir() and getcwd()
#include <libgen.h> // dirname()
#include "parse_config.hpp"

namespace graph {

	std::string ParseCmd::getConfigString(const char *option) const {

		std::string key = std::string(option);
		if (configuration.count(key) == 0) {
			std::cout << "Error: Has no option named:" << key << std::endl;
			assert(false);
		}
		// iter is type of pair
		std::map<std::string, std::string>::const_iterator iter = configuration.find(key);
		return iter -> second;
	}

	int ParseCmd::getConfigInt(const char *option) const {

		std::string key = std::string(option);
		if (configuration.count(key) == 0) {

			// special case: nedge, nvertices and nsub
			if (key.compare("nedges") == 0 || key.compare("nvertices") == 0
				|| key.compare("nsub") == 0) {

					return 0;
			}
			std::cout << "Error: Has no option named:" << key << std::endl;
			assert(false);
		}

		std::map<std::string, std::string>::const_iterator iter = configuration.find(key);
		return atoi((iter -> second).c_str());
	}

	// command line's option can override run.conf
	void ParseCmd::getConfigFromCmd() {

		if ((argc - 1) % 2 != 0) {
			std::cout << "Error: Command is wrong." << std::endl;
			assert(false);
		}

		for (int i = 1; i < argc; i += 2) {
			std::string key = std::string(argv[i]);
			std::string value = std::string(argv[i+1]);
			configuration[key] = value;
		}
	}

	// load configuration file:run.conf，e.g.totalMemory = 5000
	void ParseCmd::getConfigFromFile() {

		// modified: change the default as the hybrid_graph/bin
		chdir(dirname(argv[0])); // get the path from argv[0]
		std::ifstream inf("../run.conf");
		if (!inf.good()) {
			std::cout << "Error: can not load the configuration file." << std::endl;
			assert(false);
		}

		char line[1024] = {0};
		std::string key("");
		std::string value("");
		while (inf.getline(line,1024)) {
			// comment: # or %
			if (line[0] == '#' || line[0] == '%') {
				continue;
			}
			// split string:e.g. totalMemory = 4000
			char splitChar[] = "=";
			char *tempKey = NULL;
			char *tempValue = NULL;
			tempKey = strtok(line, splitChar);
			tempValue = strtok(NULL, splitChar);

			if(tempKey != NULL && tempValue != NULL) {
				std::string key = trim(std::string(tempKey));
				std::string value = trim(std::string(tempValue));
				configuration[key] = value;
			}
		}

		inf.close();
	}

	// TODO: 修改参数为引用，std::string &str
	std::string ParseCmd::trim(std::string str) {
		std::string trimChars = " \n\r\t";
		std::string::size_type position = str.find_first_not_of(trimChars);
		if(position != std::string::npos) {
			str.erase(0, position);
		}

		position = str.find_last_not_of(trimChars);
		if(position != std::string::npos) {
			str.erase(position+1);
		}

		return str;
	}
}
