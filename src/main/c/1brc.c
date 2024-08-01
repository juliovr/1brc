#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <windows.h>

typedef int8_t  s8;
typedef int16_t s16;
typedef int32_t s32;
typedef int64_t s64;

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef float f32;
typedef double f64;

static f32 performance_count_frequency;

inline LARGE_INTEGER win32_get_wall_clock()
{
    LARGE_INTEGER result;
    QueryPerformanceCounter(&result);
    return result;
}

inline f32 win32_get_seconds_elapsed(LARGE_INTEGER start, LARGE_INTEGER end)
{
    f32 result = (f32)(end.QuadPart - start.QuadPart) / (f32)performance_count_frequency;
    return result;
}

// u8 number_of_trailing_zeros(s64 v)
// {
//     u8 c = 64; // c will be the number of zero bits on the right
//     v &= -(s64)v;
//     if (v) c--;
//     if (v & 0x0000FFFF) c -= 16;
//     if (v & 0x00FF00FF) c -= 8;
//     if (v & 0x0F0F0F0F) c -= 4;
//     if (v & 0x33333333) c -= 2;
//     if (v & 0x55555555) c -= 1;

//     return c;
// }

// static u64 next_new_line(u64 *address) {
//     while (1) {
//         u64 value = *address;

//         u64 mask = 0x0A0A0A0A0A0A0A0AL;
//         u64 masked = value ^ mask;
//         u64 pos_new_line = (masked - 0x0101010101010101L) & (~value) & (0x8080808080808080L);

//         if (pos_new_line != 0) {
//             address += number_of_trailing_zeros(pos_new_line) >> 3; // Divide by 3 to get the index of the char
//             break;
//         }

//         address += 8;
//     }

//     return address;
// }

static int BITS_TABLE[] = { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4 };

static int count_set_bits(u64 l) {
    int count = 0;
    for (u8 i = 0; i < 16; i++) {
        u8 value = (u8)(l & 0xF);
        count += BITS_TABLE[value];
        l >>= 4;
    }

    return count;
}

typedef struct ThreadParams {
    char *file;
    int offset;
    int tid;
} ThreadParams;

DWORD thread_function(LPVOID lp_param)
{
    ThreadParams *params = (ThreadParams *)lp_param;
    
    printf("char file = %c\n", *(params->file + params->tid));
    
    return 0;
}

/*
Ref: https://learn.microsoft.com/en-us/windows/win32/memory/file-mapping
*/
int main(int argc, char **argv)
{
    LARGE_INTEGER performance_count_frequency_result;
    QueryPerformanceFrequency(&performance_count_frequency_result);
    performance_count_frequency = (f32)performance_count_frequency_result.QuadPart;

    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    printf("cores = %d\n", system_info.dwNumberOfProcessors);

    LARGE_INTEGER start = win32_get_wall_clock();

    // char *filename = "../measurements.txt";
    char *filename = "../measurements_test.txt";
    HANDLE file_handle = CreateFileA(filename, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY, NULL);
    if (file_handle == INVALID_HANDLE_VALUE) {
        fprintf(stderr, "ERROR: Cannot open file %s, error code = %d\n", filename, GetLastError());
        exit(1);
    }

    HANDLE mmap = CreateFileMappingA(file_handle, NULL, PAGE_READONLY, 0, 0, NULL);
    if (mmap == NULL) {
        fprintf(stderr, "ERROR: Cannot mmap file, error code = %d\n", GetLastError());
        exit(1);
    }

    // TODO: specify an offset to see how to split the reading between multiple threads.
    LPVOID file = MapViewOfFileEx(mmap, FILE_MAP_READ, 0, /*system_info.dwAllocationGranularity*/0, 0, NULL);
    if (file == NULL) {
        fprintf(stderr, "ERROR: Cannot map view file, error code = %d\n", GetLastError());
        exit(1);
    }

     
    int n_threads = system_info.dwNumberOfProcessors;
    HANDLE *threads = (HANDLE *)malloc(n_threads * sizeof(HANDLE));
    ThreadParams *thread_params = (ThreadParams *)malloc(n_threads * sizeof(ThreadParams));
    
    // TODO: this works, so calculate the offset (chunk_size) to process the file in thread_function
    for (int i = 0; i < n_threads; i++) {
        thread_params[i].tid = i;
        thread_params[i].file = file;
        threads[i] = CreateThread(NULL, 0, thread_function, thread_params + i, 0, 0);
    }
    
    WaitForMultipleObjects(n_threads, threads, TRUE, INFINITE);


    // printf("%s\n", (char *)file);

    // Count new lines
    int new_lines = 0;
    char *ptr = (char *)file;
    while (*ptr) {
        if (*ptr == '\n') {
            new_lines++;
        }

        ptr++;
    }
    // u64 *ptr = (u64 *)file;
    // while (*ptr) {
    //     u64 value = *ptr;
    //     u64 masked = value ^ 0x0A0A0A0A0A0A0A0AL;
    //     u64 has_new_line = (masked - 0x0101010101010101L) & (~value) & (0x8080808080808080L);
    //     new_lines += count_set_bits(has_new_line);
    //     ptr++;
    // }

    printf("lines = %d\n", new_lines);

    LARGE_INTEGER end = win32_get_wall_clock();
    
    f32 seconds_elapsed = win32_get_seconds_elapsed(start, end);
    printf("Seconds elapsed = %.2f\n", seconds_elapsed);
    

    CloseHandle(mmap);
    CloseHandle(file_handle);

    return 0;
}

/*
Results:
    Counting lines (single core):
        Seconds elapsed = 7.96
        Seconds elapsed = 7.91
*/
