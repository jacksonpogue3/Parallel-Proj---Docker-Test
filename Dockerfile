FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y build-essential gcc g++ libomp-dev

WORKDIR /app

COPY . .

# Compile all three sorting programs
RUN g++ -fopenmp quick_sort.cpp -o quick_sort && \
    g++ -fopenmp merge_sort.cpp -o merge_sort && \
    g++ -fopenmp radix_sort.cpp -o radix_sort

# Default command (you can change to quick_sort or merge_sort when needed)
CMD ["./radix_sort"]
