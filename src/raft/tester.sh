#!/bin/bash
for i in {1..100}
do
    go test -run Figure8Unreliable2C
    # go test -run Figure82C
    # go test -run Backup2B
    # go test -run UnreliableChurn2C
done