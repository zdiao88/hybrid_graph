INCFLAGS = -I/usr/local/include/ -I./	#-I表示需要搜索头文件的目录：/usr/local/include和./

CPP = g++-4.9
CPPFLAGS = -g -O3 $(INCFLAGS) -ffast-math -fopenmp -w -Wno-strict-aliasing # -Wall:显示所有警告，-w:不显示警告
DEBUGFLAGS = -g -ggdb $(INCFLAGS)
HEADERS=$(shell find . -name '*.hpp')	#使用shell命令查找所有头文件。

echo:
	echo $(HEADERS)
clean:
	@rm -rf bin/*

% : application/%.cpp # $(HEADERS)
	@mkdir -p bin
	$(CPP) $(CPPFLAGS) application/$@.cpp util/parse_config.cpp -o bin/$@

