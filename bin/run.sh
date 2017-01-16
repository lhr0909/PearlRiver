#!/bin/bash

NOHUP_LOG=xyz-processor.log
NOHUP_PID=xyz-processor.pid

#nohup java -jar dist/XYZ-Processor.jar $@ >> $NOHUP_LOG 2>&1 &
nohup java -server \
        -XX:+UseNUMA -XX:+UseCondCardMark -XX:-UseBiasedLocking -Xms1g -Xmx4g -Xss1m -XX:MaxMetaspaceSize=256m -XX:+UseParallelGC \
        -javaagent:dist/libs/aspectjweaver-1.8.10.jar \
        -cp dist/XYZ-Processor.jar:dist/libs/* com.hoolix.processor.XYZProcessorMain $@ >> $NOHUP_LOG 2>&1 &

echo $! > $NOHUP_PID
