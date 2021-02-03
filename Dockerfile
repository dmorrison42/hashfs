FROM mcr.microsoft.com/dotnet/sdk:5.0

RUN mkdir -p /mnt
RUN mkdir -p /data
COPY bin/Release/net5.0/linux-x64/publish/ App/
WORKDIR /App

ENTRYPOINT ["dotnet", "hashfs.dll", "/mnt/", "/data/hashes.db"]