#!/bin/bash

NOHUP_LOG=xyz-processor.log
NOHUP_PID=xyz-processor.pid

nohup java -jar xyz-processor-0.1.jar $@ >> $NOHUP_LOG 2>&1 &

echo $! > $NOHUP_PID
