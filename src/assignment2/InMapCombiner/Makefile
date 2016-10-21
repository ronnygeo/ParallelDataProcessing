
InMapCombiner.jar: InMapCombiner.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/InMapCombiner-all.jar InMapCombiner.jar

local: InMapCombiner.jar
	rm -rf output
	hadoop jar InMapCombiner.jar 1912-small.csv output

clean:
	rm -rf build InMapCombiner.jar output .gradle