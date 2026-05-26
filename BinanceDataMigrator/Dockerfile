FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["BinanceDataMigrator/BinanceDataMigrator.csproj", "BinanceDataMigrator/"]
COPY ["CollectorModels/CollectorModels.csproj", "CollectorModels/"]
RUN dotnet restore "BinanceDataMigrator/BinanceDataMigrator.csproj"
COPY . .
WORKDIR "/src/BinanceDataMigrator"
RUN dotnet build "BinanceDataMigrator.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "BinanceDataMigrator.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "BinanceDataMigrator.dll"]
