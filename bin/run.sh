#!/bin/bash

NOHUP_LOG=pearl-river.log
NOHUP_PID=pearl-river.pid

nohup java -jar dist/Pearl-River.jar $@ >> $NOHUP_LOG 2>&1 &

echo $! > $NOHUP_PID
