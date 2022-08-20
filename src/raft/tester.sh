echo "--- START 1000 TEST ---\n" >> ./logs/erroring.logs

#!/bin/bash
for i in {1..1000}
do
    go test -run Figure8Unreliable2C
    # go test -run Figure82C
    # go test -run Backup2B
    # go test -run UnreliableChurn2C
    echo Completed test $i/1000
done

echo "--- DONE 1000 TEST ---\n" >> ./logs/erroring.logs
