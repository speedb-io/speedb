CC=g++
CPPFLAGS=-I/opt/homebrew/opt/openssl@3/include/
LIBS=-lcurl -lcrypto -lssl -levent_openssl -levent -L/opt/homebrew/opt/openssl@3/lib/ -ljsoncpp

valdat_service: valdat_service.cpp
	$(CC) -O0 -g -std=c++17 -o $@ $^ $(CPPFLAGS) $(LIBS)
