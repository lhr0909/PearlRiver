#!/bin/bash

NOHUP_LOG=xyz-processor.log
NOHUP_PID=xyz-processor.pid

nohup java -jar dist/XYZ-Processor.jar $@ >> $NOHUP_LOG 2>&1 &

echo $! > $NOHUP_PID
