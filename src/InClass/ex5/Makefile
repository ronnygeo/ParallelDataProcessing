
DIR := $(shell basename `pwd`)

$(DIR).jar: kmeans/*.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar data.tsv.bz2

clean-output: 
	rm -rf data? data?? output

data:
	perl gendata.pl > data.tsv
	shuf data.tsv > data1.tsv
	mv data1.tsv data.tsv
	bzip2 data.tsv

clean: clean-output
	rm -rf build bin *.jar .gradle test.log

test: run
	perl test.pl | tee test.log

.PHONY: run clean-output data clean test
