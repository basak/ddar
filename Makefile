pydist: ddar.1 tbl/ddar_pb2.py

sdist: pydist
	python setup.py sdist

bdist_egg: pydist
	python setup.py bdist_egg

ddar.1: ddar.1.xml
	xmltoman ddar.1.xml > ddar.1

tbl/ddar_pb2.py: tbl/ddar.proto
	protoc --python_out=. tbl/ddar.proto

librabin.so.0: rabin.c rabin.h
	gcc -fpic -shared -o librabin.so.0 rabin.c

.PHONY: pydist sdist bdist_egg
