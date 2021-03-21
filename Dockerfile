FROM damianh/dotnet-core-lts-sdks:2

WORKDIR /repo

# https://github.com/moby/moby/issues/15858
# Docker will flatten out the file structure on COPY
# We don't want to specify each csproj either - it creates pointless layers and it looks ugly
# https://code-maze.com/aspnetcore-app-dockerfiles/
COPY ./*.sln ./

COPY ./src/*/*.csproj ./src/
RUN for file in $(ls src/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

COPY ./tests/*/*.csproj ./tests/
RUN for file in $(ls tests/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

RUN find src -name "*.csproj"  | xargs -n1 dotnet restore && \
    find tests -name "*.csproj"  | xargs -n1 dotnet restore

COPY ./assets ./assets/

COPY ./src ./src/

COPY ./tests ./tests/

WORKDIR /repo/build

COPY ./build/build.csproj .

RUN dotnet restore

COPY ./build .

WORKDIR /repo
