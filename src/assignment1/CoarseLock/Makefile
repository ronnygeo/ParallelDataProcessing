CoarseLockExec.jar: CoarseLockExec.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/CoarseLock-all.jar CoarseLock.jar

local: CoarseLock.jar
	java -jar CoarseLock.jar 1912-small.csv

delay: CoarseLock.jar
	java -jar CoarseLock.jar 1912-small.csv --delay

clean:
	rm -rf build CoarseLock.jar output .gradle

