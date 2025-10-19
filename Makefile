.PHONY: dist test

ifeq ($(OS),Windows_NT)
CFLAGS = -Wall -lws2_32
else
CFLAGS = -Wall -Werror
endif

peer.exe: peer.c
	gcc -o $@ $< $(CFLAGS)

dist: proj1.tar

proj1.tar: README.md Makefile peer.c
	tar -cvf $@ $^

test: test.exe peer.exe
	./test.out

test.exe: test.c
	gcc -Wall -Werror -o $@ $<
