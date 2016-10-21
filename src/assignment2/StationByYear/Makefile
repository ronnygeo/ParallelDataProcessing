
StationByYearList.jar: StationByYearList.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/StationByYear-all.jar StationByYearList.jar
	
local: StationByYearList.jar
	rm -rf output
	hadoop jar StationByYearList.jar in/ output

clean:
	rm -rf build StationByYearList.jar output .gradle