#!/bin/bash
while (go test -run TestFailAgree2B -race)
do
	echo "unk"
done

