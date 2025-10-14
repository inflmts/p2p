.PHONY: dist test

peer: peer.c
	gcc -Wall -Werror -o $@ $<

dist: proj1.tar

proj1.tar: README.md Makefile peer.c
	tar -cvf $@ $^

test: test.out peer
	./test.out

test.out: test.c
	gcc -Wall -Werror -o $@ $<
