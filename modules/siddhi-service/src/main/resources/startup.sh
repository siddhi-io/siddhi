#! /bin/sh


# add the libraries to the SIDDHI_CLASSPATH.
SCRIPT_PATH=${0%/*}

if [ "$0" != "$SCRIPT_PATH" ] && [ "$SCRIPT_PATH" != "" ]; then
    cd $SCRIPT_PATH
fi

DIRLIBS=dependencies/*.jar

for i in ${DIRLIBS}
do
  if [ -z "$SIDDHI_CLASSPATH" ] ; then
    SIDDHI_CLASSPATH=$i
  else
    SIDDHI_CLASSPATH="$i":$SIDDHI_CLASSPATH
  fi
done

echo classpath: $SIDDHI_CLASSPATH

java -cp siddhi-service-${pom.version}.jar":$SIDDHI_CLASSPATH:." Application
