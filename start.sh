#!/bin/sh
# exec : le PID 1 est la JVM, donc `docker stop` envoie SIGTERM a Camel et non au shell.
# Le « sleep 5 » qui suivait java ne servait a rien : il ne s'executait qu'apres l'arret de la JVM.
echo "STARTING APP (version ${CAMEL_WORKER_VERSION:-inconnue})"
exec java -Dlog4j.configurationFile=file:./log4j2.xml -jar app.jar "./camel-context.xml"
