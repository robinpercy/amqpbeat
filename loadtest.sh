#! /usr/bin/env bash

pushd scripts
go build -o load

for q in testA testB testC testD
do
        for i in $(seq 1 20)
        do
                ./load -queue=$q < data/info.json &
        done
done        
popd
