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

#define MAX_STATION_NAME_LENGTH 100
#define MAX_STATIONS 10000

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))


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


typedef struct HashMapEntry {
    char *station_name;
    int station_name_length;
    s16 min;
    s16 max;
    s16 count;
    s16 sum;
} HashMapEntry;

static HashMapEntry *all_entries;

static u32 hash(char *string, int length)
{
    u32 h = 0;
    for (int i = 0; i < length; ++i) {
        char c = string[i];
        h = (h * 31) + c;
    }
    
    return h % (MAX_STATIONS - 1);
}

static int str_equals(char *s1, int length1, char *s2, int length2)
{
    if (length1 != length2) return 0;
    
    while (--length1) {
        if (*s1++ != *s2++) return 0;
    }
    
    return 1;
}

typedef struct ThreadParams {
    u8 tid;
    u32 entries_index;
    char *file_ptr_start;
    char *file_ptr_end;
} ThreadParams;


DWORD thread_function(LPVOID lp_param)
{
    ThreadParams *params = (ThreadParams *)lp_param;
    
    // wchar_t station_name[50];
    
    int lines = 0;
    char *ptr = params->file_ptr_start;
    char *ptr_semicolon;
    char *ptr_new_line;
    while (ptr <= params->file_ptr_end) {
        ptr_semicolon = next_character(ptr, ';');
        ptr_new_line = next_character(ptr_semicolon, '\n');
        
        if (ptr_new_line) {
            int station_name_length = (int)(ptr_semicolon - ptr);
            int value_length = (int)(ptr_new_line - ptr_semicolon - 1);
            
            // Parse station_name
            // TODO: fix station_name_length. For wide characters it consider the bytes, not chars, so 2-bytes char is length 2 instead of 1.
            // mbstowcs(station_name, ptr, station_name_length);
            
            // Parse value. The range goes from -99.9 to 99.9, so the decimal point always goes into 1 or second index (excluding the sign).
            // This is an optimize version taking advantage of this particular case.
            s16 value = 0;
            char *ptr_value = ptr_semicolon + 1;
            s8 sign = 1;
            if (ptr_value[0] == '-') {
                sign = (s8)-1;
                ptr_value++;
            }
            
            if (ptr_value[1] == '.') {
                value = ((ptr_value[0] - '0') * 10) + (ptr_value[2] - '0');
            } else if (ptr_value[2] == '.') {
                value = ((ptr_value[0] - '0') * 100) + ((ptr_value[1] - '0') * 10) + (ptr_value[3] - '0');
            }
            
            value *= sign;
            
            
            // printf("value = %d\n", value);
            
            // Get entry if exists
            int entries_base_index = params->tid * MAX_STATIONS;
            u32 entry_hash = hash(ptr, station_name_length);
            HashMapEntry *entry = all_entries + entries_base_index + entry_hash;
            while (entry->station_name && !str_equals(entry->station_name, entry->station_name_length,
                                                      ptr, station_name_length))
            {
                ++entry_hash;
                ++entry;
            }
            
            if (!entry->station_name) {
                entry->station_name = ptr;
                entry->station_name_length = station_name_length;
                
                // Insert new entry
                all_entries[entries_base_index + entry_hash] = *entry;
            }
            
            entry->min = MIN(entry->min, value);
            entry->max = MAX(entry->max, value);
            entry->count++;
            entry->sum += value;
            
            // printf("station name = %.*ls, value = %.*s\n", station_name_length, station_name, value_length, ptr_semicolon + 1);
        }
        
        ptr = ptr_new_line + 1;
    }
    
    return 0;
}

static inline int char_at(char *s, int length, int d)
{
    if (d < length) {
        return s[d];
    } else {
        return -1;
    }
}

static void sort(HashMapEntry result[], int lo, int hi, int d)
{
    if (hi <= lo) return;
    
    int lt = lo;
    int gt = hi;
    
    if (!result[lo].station_name) return;
    
    int v = char_at(result[lo].station_name, result[lo].station_name_length, d);
    int i = lo + 1;
    while (i <= gt) {
        if (!result[i].station_name) {
            ++i;
            continue;
        }
        
        int t = char_at(result[i].station_name, result[i].station_name_length, d);
        if (t < v) {
            HashMapEntry temp = result[lt];
            result[lt] = result[i];
            result[i] = temp;
            
            ++lt;
            ++i;
        } else if (t > v) {
            HashMapEntry temp = result[i];
            result[i] = result[gt];
            result[gt] = temp;
            
            --gt;
        } else {
            ++i;
        }
    }
    
    sort(result, lo, lt-1, d);
    if (v >= 0) {
        sort(result, lt, gt, d+1);
    }
    sort(result, gt+1, hi, d);
}

/*
Ref: https://learn.microsoft.com/en-us/windows/win32/memory/file-mapping
*/
int main(int argc, char **argv)
{
    setlocale(LC_ALL, "en_US.UTF8");

    LARGE_INTEGER performance_count_frequency_result;
    QueryPerformanceFrequency(&performance_count_frequency_result);
    performance_count_frequency = (f32)performance_count_frequency_result.QuadPart;

    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    
    LARGE_INTEGER start = win32_get_wall_clock();

#if 1
    int n_threads = system_info.dwNumberOfProcessors;
    char *filename = "../measurements.txt";
#else
    int n_threads = 1;
    char *filename = "../measurements_test.txt";
#endif
    HANDLE file_handle = CreateFileA(filename, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY, NULL);
    if (file_handle == INVALID_HANDLE_VALUE) {
        fprintf(stderr, "ERROR: Cannot open file %s, error code = %d\n", filename, GetLastError());
        exit(1);
    }

    LARGE_INTEGER file_size_result;
    GetFileSizeEx(file_handle, &file_size_result);
    s64 file_size = file_size_result.QuadPart;

    HANDLE mmap = CreateFileMappingA(file_handle, NULL, PAGE_READONLY, file_size_result.HighPart, file_size_result.LowPart, NULL);
    if (mmap == NULL) {
        fprintf(stderr, "ERROR: Cannot mmap file, error code = %d\n", GetLastError());
        exit(1);
    }
    
    char *file = (char *)MapViewOfFileEx(mmap, FILE_MAP_READ, 0, 0, file_size, 0);
    if (file == NULL) {
        fprintf(stderr, "ERROR: Cannot map view file, error code = %d\n", GetLastError());
        exit(1);
    }
            
    HANDLE *threads = (HANDLE *)malloc(n_threads * sizeof(HANDLE));
    ThreadParams *thread_params = (ThreadParams *)malloc(n_threads * sizeof(ThreadParams));
    
    HashMapEntry empty = {0};
    u32 entries_size = MAX_STATIONS * n_threads;
    all_entries = (HashMapEntry *)malloc(entries_size * sizeof(HashMapEntry));
    for (u32 i = 0; i < entries_size; ++i) {
        all_entries[i] = empty;
    }
    
    s64 chunk_size = file_size / n_threads;
    char *file_end_address = file + file_size;
    char *file_ptr = file;
    for (int i = 0; i < n_threads; ++i) {
        thread_params[i].tid = (u8)i;
        thread_params[i].file_ptr_start = file_ptr;
        thread_params[i].file_ptr_end = (i == n_threads - 1) ? file_end_address : next_character(file_ptr + chunk_size, '\n');
        thread_params[i].entries_index = 0;
        
        file_ptr = thread_params[i].file_ptr_end + 1;
        
        threads[i] = CreateThread(NULL, 0, thread_function, thread_params + i, 0, 0);
    }
    
    WaitForMultipleObjects(n_threads, threads, TRUE, INFINITE);

    
    // Join the results
    HashMapEntry result[MAX_STATIONS];
    memset(result, 0, MAX_STATIONS * sizeof(HashMapEntry));
    
    for (int i = 0; i < n_threads; ++i) {
        int entries_base_index = thread_params[i].tid * MAX_STATIONS;
        for (int j = 0; j < MAX_STATIONS; ++j) {
            HashMapEntry entry = all_entries[entries_base_index + j];
            if (entry.station_name) {
                u32 entry_hash = hash(entry.station_name, entry.station_name_length);
                HashMapEntry *result_entry = result + entry_hash;
                
                while (result_entry->station_name && !str_equals(result_entry->station_name, result_entry->station_name_length,
                                                                 entry.station_name, entry.station_name_length))
                {
                    ++entry_hash;
                }
                
                if (!result_entry->station_name) {
                    result_entry->station_name = entry.station_name;
                    result_entry->station_name_length = entry.station_name_length;
                }
                
                result_entry->min = MIN(result_entry->min, entry.min);
                result_entry->max = MAX(result_entry->max, entry.max);
                result_entry->count += entry.count;
                result_entry->sum += entry.sum;
            }
        }
    }
    

    sort(result, 0, MAX_STATIONS - 1, 0);
    
    printf("{\n");
    for (int i = 0; i < MAX_STATIONS; ++i) {
        HashMapEntry entry = result[i];
        if (entry.station_name) {
            // TODO: Check why the average value is wrong!!
            printf("%.*s=%.1f/%.1f/%.1f\n", entry.station_name_length, entry.station_name, 
                                      (double)entry.min / 10.0, 
                                      ((double)entry.sum / (double)entry.count) / 10.0, 
                                      (double)entry.max / 10.0);
        }
    }
    printf("}\n");
    

    LARGE_INTEGER end = win32_get_wall_clock();
    
    f32 seconds_elapsed = win32_get_seconds_elapsed(start, end);
    printf("Seconds elapsed = %.2f\n", seconds_elapsed);
    

    CloseHandle(mmap);
    CloseHandle(file_handle);

    return 0;
}

/*
Results counting lines
    Single core:
        Seconds elapsed = 7.96
        Seconds elapsed = 7.91
    
    Using all cores available (20 in my machine):
        Seconds elapsed = 8.90
        Seconds elapsed = 1.03
        Seconds elapsed = 1.02
        Seconds elapsed = 0.95
        Seconds elapsed = 0.98

Results calculating results in their own hashmaps:
    Seconds elapsed = 3.26
    
Results using all_entries as THE hashmap:
    Seconds elapsed = 2.39


*/
