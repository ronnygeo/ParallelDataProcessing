FineLockExec.jar: FineLockExec.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/FineLock-all.jar FineLock.jar

local: FineLock.jar
	java -jar FineLock.jar 1912-small.csv

delay: FineLock.jar
	java -jar FineLock.jar 1912-small.csv --delay

clean:
	rm -rf build FineLock.jar output .gradle

