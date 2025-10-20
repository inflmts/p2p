.PHONY: dist test

ifneq ($(OS),Windows_NT)
CFLAGS := -Wall -Werror
else
CFLAGS := -Wall -Werror -lws2_32
endif

peer.exe: peer.c
	gcc -o $@ $< $(CFLAGS)

dist: proj1.tar

proj1.tar: README.md Makefile peer.c
	tar -cvf $@ $^

test: test.exe peer.exe
	./test.exe

test.exe: test.c
	gcc -o $@ $< $(CFLAGS)
