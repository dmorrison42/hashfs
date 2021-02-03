#!/usr/bin/sh

dotnet publish -c Release -r linux-x64
docker build -t hashfs-image -f Dockerfile .
docker save -o hashfs.tar hashfs-image:latest