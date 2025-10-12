.PHONY: dist

peer: peer.c
	gcc -Wall -o $@ $<

dist: proj1.tar
proj1.tar: README.md Makefile peer.c
	tar -cvf $@ $^
