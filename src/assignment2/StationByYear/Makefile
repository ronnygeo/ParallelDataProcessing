
StationByYearList.jar: StationByYearList.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/StationByYear-all.jar StationByYearList.jar
	zip -d StationByYearList.jar META-INF/LICENSE
	zip -d StationByYearList.jar LICENSE
	jar tvf StationByYearList.jar | grep -i license
	
local: StationByYearList.jar
	rm -rf output
	hadoop jar StationByYearList.jar file:///in/ file:///output

clean:
	rm -rf build StationByYearList.jar output .gradle
