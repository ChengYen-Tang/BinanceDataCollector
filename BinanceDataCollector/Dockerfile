#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["BinanceDataCollector/BinanceDataCollector.csproj", "BinanceDataCollector/"]
COPY ["CollectorModels/CollectorModels.csproj", "CollectorModels/"]
RUN dotnet restore "BinanceDataCollector/BinanceDataCollector.csproj"
COPY . .
WORKDIR "/src/BinanceDataCollector"
RUN dotnet build "BinanceDataCollector.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "BinanceDataCollector.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "BinanceDataCollector.dll"]