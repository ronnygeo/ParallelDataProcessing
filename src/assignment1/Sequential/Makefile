SequentialExec.jar: SequentialExec.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/Sequential-all.jar Sequential.jar

local: Sequential.jar
	java -jar Sequential.jar 1912-small.csv

delay: Sequential.jar
	java -jar Sequential.jar 1912-small.csv --delay

clean:
	rm -rf build Sequential.jar output .gradle

