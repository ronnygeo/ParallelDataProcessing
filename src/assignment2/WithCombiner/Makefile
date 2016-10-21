
WithCombiner.jar: WithCombiner.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/WithCombiner-all.jar WithCombiner.jar

local: WithCombiner.jar
	rm -rf output
	hadoop jar WithCombiner.jar 1912-small.csv output

clean:
	rm -rf build WithCombiner.jar output .gradle