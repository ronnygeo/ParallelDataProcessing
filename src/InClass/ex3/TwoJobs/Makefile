
TwoJobs.jar: TwoJobs.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/TwoJobs-all.jar TwoJobs.jar

run: TwoJobs.jar
	rm -rf output
	hadoop jar TwoJobs.jar alice.txt output

clean:
	rm -rf build TwoJobs.jar output .gradle
