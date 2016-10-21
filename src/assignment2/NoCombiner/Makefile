
NoCombiner.jar: NoCombiner.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/NoCombiner-all.jar NoCombiner.jar

local: NoCombiner.jar
	rm -rf output
	hadoop jar NoCombiner.jar 1912-small.csv output

clean:
	rm -rf build NoCombiner.jar output .gradle