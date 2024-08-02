#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <wchar.h>
#include <locale.h>
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

static char *next_character(char *ptr, char c)
{
    while (*ptr && *ptr != c) {
        ptr++;
    }
    
    return ptr;
}

/*
Returns the address of the next new line.
*/
static char *next_new_line(char *ptr)
{
    return next_character(ptr, '\n');
}

typedef struct ThreadParams {
    char *file_ptr_start;
    char *file_ptr_end;
} ThreadParams;

DWORD thread_function(LPVOID lp_param)
{
    ThreadParams *params = (ThreadParams *)lp_param;
    
    wchar_t station_name[50];
    
    int lines = 0;
    char *ptr = params->file_ptr_start;
    char *ptr_semicolon;
    char *ptr_new_line;
    while (ptr <= params->file_ptr_end) {
        ptr_semicolon = next_character(ptr, ';');
        ptr_new_line = next_new_line(ptr_semicolon);
        
        if (ptr_new_line) {
            int station_name_length = (int)(ptr_semicolon - ptr);
            int value_length = (int)(ptr_new_line - ptr_semicolon - 1);
            
            // Parse station_name
            // TODO: fix station_name_length. For wide characters it consider the bytes, not chars, so 2-bytes char is length 2 instead of 1.
            mbstowcs(station_name, ptr, station_name_length);
            
            // Parse value
            s16 value = 0;
            char *ptr_value = ptr_new_line - 1;
            int dec = 1;
            while (ptr_value > ptr_semicolon) {
                char c = *ptr_value--;
                if (c == '-') {
                    value *= -1;
                } else if (c == '.') {
                    continue;
                } else {
                    value += (s16)((int)(c - '0') * dec);
                    dec *= 10;
                }
            }
            
            
            printf("station name = %.*ls, value = %.*s\n", station_name_length, station_name, value_length, ptr_semicolon + 1);
        }
        
        ptr = ptr_new_line + 1;
    }
    
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

    LPVOID file = MapViewOfFileEx(mmap, FILE_MAP_READ, 0, /*system_info.dwAllocationGranularity*/0, 0, NULL);
    if (file == NULL) {
        fprintf(stderr, "ERROR: Cannot map view file, error code = %d\n", GetLastError());
        exit(1);
    }
    
    LARGE_INTEGER file_size_result;
    GetFileSizeEx(file_handle, &file_size_result);
    s64 file_size = file_size_result.QuadPart;
    

    setlocale(LC_ALL, "en_US.UTF8");
        
    // int n_threads = system_info.dwNumberOfProcessors;
    int n_threads = 1;
    HANDLE *threads = (HANDLE *)malloc(n_threads * sizeof(HANDLE));
    ThreadParams *thread_params = (ThreadParams *)malloc(n_threads * sizeof(ThreadParams));
    
    s64 chunk_size = file_size / n_threads;
    char *file_end_address = ((char *)file) + file_size;
    char *file_ptr = (char *)file;
    for (int i = 0; i < n_threads; i++) {
        thread_params[i].file_ptr_start = file_ptr;
        thread_params[i].file_ptr_end = (i == n_threads - 1) ? file_end_address : next_new_line(file_ptr + chunk_size);
        
        file_ptr = thread_params[i].file_ptr_end + 1;
        
        threads[i] = CreateThread(NULL, 0, thread_function, thread_params + i, 0, 0);
    }
    
    WaitForMultipleObjects(n_threads, threads, TRUE, INFINITE);
    
    // int lines = 0;
    // for (int i = 0; i < n_threads; i++) {
    //     lines += thread_params[i].lines;
    // }

    // printf("lines = %d\n", lines);

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
    
    Using all cores available (20 in my machine):
        Seconds elapsed = 8.90
        Seconds elapsed = 1.03
        Seconds elapsed = 1.02
        Seconds elapsed = 0.95
        Seconds elapsed = 0.98
        Seconds elapsed = 0.99
*/
