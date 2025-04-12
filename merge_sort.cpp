// === BATCHED MERGE SORT ONLY ===
// Processes 1,000,000 entries at a time from a large CSV (trips.csv),

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>

#define BATCH_SIZE 1000000
#define MAX_LINE_LENGTH 256

void merge(int arr[], int l, int m, int r) {
    int n1 = m - l + 1;
    int n2 = r - m;
    int L[n1], R[n2];
    for (int i = 0; i < n1; i++) L[i] = arr[l + i];
    for (int j = 0; j < n2; j++) R[j] = arr[m + 1 + j];

    int i = 0, j = 0, k = l;
    while (i < n1 && j < n2)
        arr[k++] = (L[i] <= R[j]) ? L[i++] : R[j++];
    while (i < n1) arr[k++] = L[i++];
    while (j < n2) arr[k++] = R[j++];
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
            break;
        }
    }
    fclose(fp);
}

int extractDepartureTime(char *line) {
    char *token = strtok(line, ",");
    int field = 0;
    while (token) {
        if (field == 5) return atoi(token);
        token = strtok(NULL, ",");
        field++;
    }
    return -1;
}

int main() {
    FILE *fp = fopen("trips.csv", "r");

    if (!fp) { perror("File open failed"); return 1; }

    char line[MAX_LINE_LENGTH];
    if (!fgets(line, sizeof(line), fp)) { fclose(fp); return 1; } // Skip header

    int batch[BATCH_SIZE];
    int index = 0, batch_num = 1;

    while (fgets(line, sizeof(line), fp)) {
        int time = extractDepartureTime(line);
        if (time >= 0) batch[index++] = time;

        if (index == BATCH_SIZE) {
            double start = omp_get_wtime();
            omp_set_num_threads(4);
            #pragma omp parallel
            {
                #pragma omp single
                mergeSortParallel(batch, 0, index - 1, 0);
            }
            double end = omp_get_wtime();

            printf("Batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
            printMemoryUsageKB();

            index = 0;
            batch_num++;
        }
    }

    if (index > 0) {
        double start = omp_get_wtime();
        omp_set_num_threads(4);
        #pragma omp parallel
        {
            #pragma omp single
            mergeSortParallel(batch, 0, index - 1, 0);
        }
        double end = omp_get_wtime();
        printf("Final batch #%d: Sorted %d entries in %.6f seconds\n", batch_num, index, end - start);
        printMemoryUsageKB();
    }

    fclose(fp);
    return 0;
}
