PageRank.jar: PageRank.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/assignment5-all.jar PageRank.jar
	zip -d PageRank.jar META-INF/LICENSE
	zip -d PageRank.jar LICENSE
	jar tvf PageRank.jar | grep -i license
	
local: PageRank.jar
	rm -rf output
	hadoop jar PageRank.jar file:///in/ file:///output

clean:
	rm -rf build PageRank.jar output .gradle
