#!/bin/bash

if [ $# -lt 2 ]; then
	echo "Usage: simulation.sh -f file"
	echo "Example: simulation.sh -f lusiadas.txt"
	exit 0
fi

gnome-terminal --title="Coordinator" -e "python3.7 coordinator.py -f $2"
gnome-terminal --title="Backup" -e "python3.7 coordinator.py -f $2"

sleep 5

for i in {1..4} 
do
	gnome-terminal --title="Worker $i" -e "python3.7 worker.py --id $i" &
done
