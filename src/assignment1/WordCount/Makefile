
WordCount.jar: WordCount.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/WordCount-all.jar WordCount.jar

run: WordCount.jar
	rm -rf output
	hadoop jar WordCount.jar alice.txt output

clean:
	rm -rf build WordCount.jar output .gradle
