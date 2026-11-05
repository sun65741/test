epollhttp:epollhttp.cpp wrap.cpp wrap.h
	g++ epollhttp.cpp wrap.cpp wrap.h -o epollhttp
clean:
	rm -f epollhttp 