#!/bin/bash
for i in {1..50}
do
    # go test -run Figure8Unreliable2C
    # go test -run Figure82C
    go test -run Backup2B
done
