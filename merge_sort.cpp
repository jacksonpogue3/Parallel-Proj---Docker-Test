// === BATCHED MERGE SORT ONLY ===
// Processes 1,000,000 entries at a time from a large CSV (trips.csv),

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>

#define BATCH_SIZE 1000000
#define NUM_BATCHES 13
#define MAX_LINE_LENGTH 256

void merge(int arr[], int l, int m, int r) {
    int n1 = m - l + 1;
    int n2 = r - m;
    int *L = (int *)malloc(sizeof(int) * n1);
    int *R = (int *)malloc(sizeof(int) * n2);

    for (int i = 0; i < n1; i++) L[i] = arr[l + i];
    for (int j = 0; j < n2; j++) R[j] = arr[m + 1 + j];

    int i = 0, j = 0, k = l;
    while (i < n1 && j < n2)
        arr[k++] = (L[i] <= R[j]) ? L[i++] : R[j++];
    while (i < n1) arr[k++] = L[i++];
    while (j < n2) arr[k++] = R[j++];

    free(L);
    free(R);
}

void mergeSortParallel(int arr[], int l, int r, int depth) {
    if (l < r) {
        int m = (l + r) / 2;
        if (depth < 3) {
            #pragma omp task shared(arr)
            mergeSortParallel(arr, l, m, depth + 1);
            #pragma omp task shared(arr)
            mergeSortParallel(arr, m + 1, r, depth + 1);
            #pragma omp taskwait
        } else {
            mergeSortParallel(arr, l, m, depth + 1);
            mergeSortParallel(arr, m + 1, r, depth + 1);
        }
        merge(arr, l, m, r);
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
    if (!batch) {
        printf("Memory allocation failed!\n");
        fclose(fp);
        return 1;
    }

    int index = 0;
    int batch_num = 1;
    int total_valid_lines = 0;

    while (fgets(line, sizeof(line), fp) && batch_num <= NUM_BATCHES) {
        char *line_copy = strdup(line);
        if (!line_copy) continue;

        int time = extractDepartureTime(line_copy);
        free(line_copy);

        if (time < 0) continue;

        batch[index++] = time;
        total_valid_lines++;

        if (index == BATCH_SIZE) {
            double start = omp_get_wtime();
            omp_set_num_threads(4);  // Change to 1 or 2 as needed
            #pragma omp parallel
            {
                #pragma omp single
                mergeSortParallel(batch, 0, index - 1, 0);
            }
            double end = omp_get_wtime();

            printf("Batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
            fflush(stdout);
            printMemoryUsageKB();

            index = 0;
            batch_num++;
        }
    }

    free(batch);
    fclose(fp);

    printf("Total valid lines processed: %d\n", total_valid_lines);
    fflush(stdout);
    return 0;
}

