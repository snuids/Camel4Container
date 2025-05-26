#!/bin/sh

echo 'STARTING APP'
cd target
#java -D -Dlog4j.configurationFile=file:./log4j2.xml -jar app.jar "./camel-context.xml"
java -Dlog4j.debug=true -Dlog4j.configuration=file:/Users/snuids/Documents/GitHub/Camel4Container/data/log4j.properties  -jar ./camel-1.0-SNAPSHOT.jar /Users/snuids/ICTCS/APSB/Configs/APSB.xml


sleep 5
