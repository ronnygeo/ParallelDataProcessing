NoSharingExec.jar: NoSharingExec.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/NoSharing-all.jar NoSharing.jar

local: NoSharing.jar
	java -jar NoSharing.jar 1912-small.csv

delay: NoSharing.jar
	java -jar NoSharing.jar 1912-small.csv --delay

clean:
	rm -rf build NoSharing.jar output .gradle

