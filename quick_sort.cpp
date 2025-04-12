// === BATCHED QUICK SORT ONLY ===
// Processes 1,000,000 entries at a time from a large CSV (trips.csv),

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <stdbool.h>

#define BATCH_SIZE 13000000
#define MAX_LINE_LENGTH 256

void swap(int *a, int *b) {
    int t = *a;
    *a = *b;
    *b = t;
}

int partition(int arr[], int low, int high) {
    int pivot = arr[high];
    int i = low - 1;
    for (int j = low; j < high; j++) {
        if (arr[j] < pivot) {
            i++;
            swap(&arr[i], &arr[j]);
        }
    }
    swap(&arr[i + 1], &arr[high]);
    return i + 1;
}

void quickSortParallel(int arr[], int low, int high, int depth) {
    if (low < high) {
        int pi = partition(arr, low, high);
        if (depth < 3) {
            #pragma omp task shared(arr)
            quickSortParallel(arr, low, pi - 1, depth + 1);
            #pragma omp task shared(arr)
            quickSortParallel(arr, pi + 1, high, depth + 1);
            #pragma omp taskwait
        } else {
            quickSortParallel(arr, low, pi - 1, depth + 1);
            quickSortParallel(arr, pi + 1, high, depth + 1);
        }
    }
}

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

    // Walk through the line, find the start of the 6th field
    while (*line && commas < 5) {
        if (*line == ',') commas++;
        line++;
    }

    if (commas < 5) return -1; // Not enough fields

    // We're now at the start of column 6
    end = line;
    while (*end && *end != ',') end++;

    // Temporarily null-terminate this field
    char temp = *end;
    *end = '\0';

    // Strip quotes and newlines
    if (line[0] == '"') line++;
    line[strcspn(line, "\"\r\n")] = '\0';

    if (line[0] == '\0') return -1;

    char *parse_end;
    int result = strtol(line, &parse_end, 10);
    if (*parse_end != '\0') return -1;

    *end = temp; // restore line
    return result;
}


int main() {
    printf("Program started...\n");
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
    if (batch == NULL) {
        printf("Memory allocation failed!\n");
        fclose(fp);
        return 1;
    }

    int index = 0;
    int batch_num = 1;
    int total_valid_lines = 0;

    while (fgets(line, sizeof(line), fp)) {
        char *line_copy = strdup(line);
        if (!line_copy) continue;

        int time = extractDepartureTime(line_copy);
        free(line_copy);

        if (time < 0) continue;

        batch[index++] = time;
        total_valid_lines++;

        if (index == BATCH_SIZE) {
            bool already_sorted = true;
            for (int i = 1; i < index; i++) {
                if (batch[i] < batch[i - 1]) {
                    already_sorted = false;
                    break;
                }
            }

            if (already_sorted) {
                printf("Batch #%d was already sorted!\n", batch_num);
                fflush(stdout);
            }

            double start = omp_get_wtime();
            omp_set_num_threads(2);
            #pragma omp parallel
            {
                #pragma omp single
                quickSortParallel(batch, 0, index - 1, 0);
            }
            double end = omp_get_wtime();

            printf("Batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
            fflush(stdout);
            printMemoryUsageKB();

            index = 0;
            batch_num++;
        }
    }

    if (index > 0) {
        bool already_sorted = true;
        for (int i = 1; i < index; i++) {
            if (batch[i] < batch[i - 1]) {
                already_sorted = false;
                break;
            }
        }

        if (already_sorted) {
            printf("Final batch #%d was already sorted!\n", batch_num);
            fflush(stdout);
        }

        double start = omp_get_wtime();
        omp_set_num_threads(4);
        #pragma omp parallel
        {
            #pragma omp single
            quickSortParallel(batch, 0, index - 1, 0);
        }
        double end = omp_get_wtime();

        printf("Final batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
        fflush(stdout);
        printMemoryUsageKB();
    }

    printf("Total valid lines processed: %d\n", total_valid_lines);
    fflush(stdout);

    free(batch);
    fclose(fp);
    return 0;
}











