.PHONY: dist test

ifeq ($(OS),Windows_NT)
peer.exe: peer.c
	gcc -Wall -Werror -o $@ $< -lws2_32
else
peer: peer.c
	gcc -Wall -Werror -o $@ $<
endif

dist: dist.tar

dist.tar: README.md Makefile peer.c
	tar -cvf $@ $^

test: peer
	cd work && ../peer
