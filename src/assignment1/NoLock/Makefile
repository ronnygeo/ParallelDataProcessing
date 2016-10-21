NoLockExec.jar: NoLockExec.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/NoLock-all.jar NoLock.jar

local: NoLock.jar
	java -jar NoLock.jar 1912-small.csv

delay: NoLock.jar
	java -jar NoLock.jar 1912-small.csv --delay

clean:
	rm -rf build NoLock.jar output .gradle

