#!/bin/bash

echo bye bye bye

docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

docker rmi $(docker images -a -q)
