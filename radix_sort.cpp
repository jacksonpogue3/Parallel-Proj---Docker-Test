
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <stdbool.h>

#define BATCH_SIZE 1000000
#define MAX_LINE_LENGTH 256
#define MAX_DIGIT 10

void printMemoryUsageKB() {
    FILE *fp = fopen("/proc/self/status", "r");
    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            printf("RAM usage:%s", line + 6);
            fflush(stdout);
            break;
        }
    }
    fclose(fp);
}

int extractDepartureTime(char *line) {
    int commas = 0;
    char *start = line;
    char *end;

    while (*line && commas < 5) {
        if (*line == ',') commas++;
        line++;
    }

    if (commas < 5) return -1;

    end = line;
    while (*end && *end != ',') end++;

    char temp = *end;
    *end = '\0';

    if (line[0] == '"') line++;
    line[strcspn(line, "\"\r\n")] = '\0';

    if (line[0] == '\0') return -1;

    char *parse_end;
    int result = strtol(line, &parse_end, 10);
    if (*parse_end != '\0') return -1;

    *end = temp;
    return result;
}

int getMax(int arr[], int n) {
    int max = arr[0];
    for (int i = 1; i < n; i++)
        if (arr[i] > max)
            max = arr[i];
    return max;
}

void countSort(int arr[], int n, int exp) {
    int output[n];
    int count[MAX_DIGIT] = {0};


    for (int i = 0; i < n; i++)
        count[(arr[i] / exp) % 10]++;

    for (int i = 1; i < MAX_DIGIT; i++)
        count[i] += count[i - 1];

    for (int i = n - 1; i >= 0; i--) {
        int digit = (arr[i] / exp) % 10;
        output[count[digit] - 1] = arr[i];
        count[digit]--;
    }

    for (int i = 0; i < n; i++)
        arr[i] = output[i];
}

void radixSort(int arr[], int n) {
    int max = getMax(arr, n);
    for (int exp = 1; max / exp > 0; exp *= 10)
        countSort(arr, n, exp);
}

int main() {
    printf("Program started...\n");
    omp_set_num_threads(4); // 👈 Change this to 2 or 4 later
    fflush(stdout);

    FILE *fp = fopen("trips.csv", "r");
    if (!fp) {
        perror("File open failed");
        return 1;
    }
    printf("File opened successfully\n");
    fflush(stdout);

    char line[MAX_LINE_LENGTH];
    if (!fgets(line, sizeof(line), fp)) {
        fclose(fp);
        return 1;
    }

    int *batch = (int *)malloc(sizeof(int) * BATCH_SIZE);
    if (!batch) {
        printf("Memory allocation failed\n");
        fclose(fp);
        return 1;
    }

    int index = 0, batch_num = 1, total_valid = 0;

    while (fgets(line, sizeof(line), fp) && batch_num <= 13) {
        char *copy = strdup(line);
        if (!copy) continue;

        int time = extractDepartureTime(copy);
        free(copy);

        if (time < 0) continue;

        batch[index++] = time;
        total_valid++;

        if (index == BATCH_SIZE) {
            double start = omp_get_wtime();
            radixSort(batch, index);
            double end = omp_get_wtime();

            printf("Batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
            fflush(stdout);
            printMemoryUsageKB();

            index = 0;
            batch_num++;
        }
    }

    if (index > 0 && batch_num <= 13) {
        double start = omp_get_wtime();
        radixSort(batch, index);
        double end = omp_get_wtime();
        printf("Final batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
        printMemoryUsageKB();
    }

    printf("Total valid lines processed: %d\n", total_valid);
    fflush(stdout);
    free(batch);
    fclose(fp);
    return 0;
}
