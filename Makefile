all:
	g++ mr-pr-cpp.cpp /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o mr-pr-cpp.o
	
run_test1:
	time ./mr-pr-cpp.o ./tests/cubical.txt ./outputs/cubical-pr-cpp.txt

run_test2:
	time ./mr-pr-cpp.o ./tests/dodecahedral.txt ./outputs/dodecahedral-pr-cpp.txt

run_test3:
	time ./mr-pr-cpp.o ./tests/coxeter.txt ./outputs/coxeter-pr-cpp.txt

run_test4:
	time ./mr-pr-cpp.o ./tests/erdos10000.txt ./outputs/erdos10000-pr-cpp.txt
	
run_test5:
	time ./mr-pr-cpp.o ./tests/barabasi20000.txt ./outputs/barabasi20000-pr-cpp.txt
