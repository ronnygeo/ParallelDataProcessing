
ReducerSort.jar: ReducerSort.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/ReducerSort-all.jar ReducerSort.jar

run: ReducerSort.jar
	rm -rf output
	hadoop jar ReducerSort.jar alice.txt output

clean:
	rm -rf build ReducerSort.jar output .gradle
