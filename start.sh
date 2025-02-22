#!/bin/sh

echo 'STARTING APP'
java -D -Dlog4j.configurationFile=file:./log4j2.xml -jar app.jar "./camel-context.xml"
sleep 5