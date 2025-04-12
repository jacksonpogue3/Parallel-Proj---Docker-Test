FROM ubuntu:20.04

RUN apt-get update && apt-get install -y build-essential gcc g++ libomp-dev

WORKDIR /app

COPY . .

RUN g++ -fopenmp quick_sort.cpp -o quick_sort

CMD ["./quick_sort"]
