#!/bin/bash
while (go test -run 2A -race)
do
	echo "unk"
done

