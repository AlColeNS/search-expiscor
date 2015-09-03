#!/bin/bash
#
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home
export JAVA_CMD=$JAVA_HOME/bin/java
#
export APP_HOME=/Users/acole/expiscor/apl/src/connector/con_fs
#
export JAR_FILE=target/con_fs-1.0-jar-with-dependencies.jar
#
pushd $APP_HOME
echo Starting Discover File Share Connector Service [$JAR_FILE]
#
rm -f $APP_HOME/log/con_fs.log
rm -f $APP_HOME/log/start-con_fs.log
rm -f $APP_HOME/log/service-tracker.xml 
#
nohup $JAVA_CMD -Xms256m -Xmx1024m -jar $JAR_FILE -run cfs  > $APP_HOME/log/start-con_fs.log 2>/dev/null &
popd
exit 0

