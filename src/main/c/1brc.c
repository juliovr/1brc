#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <windows.h>
#include <nmmintrin.h>

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


static inline int str_equals(char *s1, int length1, char *s2, int length2)
{
    if (length1 != length2) return 0;
    
    __m128i a = _mm_loadu_si128((const __m128i *)s1);
    __m128i b = _mm_loadu_si128((const __m128i *)s2);
    return _mm_cmpestrc(a, length1, b, length2, _SIDD_CMP_EQUAL_ORDERED);
}

static inline char *next_character(char *ptr, char c)
{
    int result = 16; // 16 is the max value returned by _mm_cmpistri (when the character is not found)
    while (*ptr && result == 16) {
        __m128i a = _mm_loadu_si128((const __m128i *)ptr);
        __m128i b = _mm_set1_epi8(c);
        result = _mm_cmpistri(a, b, _SIDD_CMP_EQUAL_EACH);
        
        ptr += result;
    }
    
    return ptr;
}


typedef struct HashMapEntry {
    char *station_name;
    u8 station_name_length;
    u32 count;
    s32 sum;
    s16 min;
    s16 max;
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


typedef struct ThreadParams {
    u8 tid;
    u32 entries_index;
    char *file_ptr_start;
    char *file_ptr_end;
} ThreadParams;


DWORD thread_function(LPVOID lp_param)
{
    ThreadParams *params = (ThreadParams *)lp_param;
    
    int lines = 0;
    char *ptr = params->file_ptr_start;
    char *ptr_semicolon;
    char *ptr_new_line;
    while (ptr < params->file_ptr_end) {
        ptr_semicolon = next_character(ptr, ';');
        ptr_new_line = next_character(ptr_semicolon, '\n');
        
        if (ptr_new_line) {
            u8 station_name_length = (u8)(ptr_semicolon - ptr);
            
            // The value's range is between -99.9 to 99.9, so there is no need to make a general function.
            // This particular one is faster.
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
        }
        
        ptr = ptr_new_line + 1;
    }
    
    return 0;
}

/* QuickSort compare function callback */
int cmp(const void *ptr1, const void *ptr2)
{
    HashMapEntry *entry1 = (HashMapEntry *)ptr1;
    HashMapEntry *entry2 = (HashMapEntry *)ptr2;
    
    if (entry1->station_name && !entry2->station_name) {
        return -1;
    }
    
    if (!entry1->station_name && entry2->station_name) {
        return 1;
    }
    
    int length = MIN(entry1->station_name_length, entry2->station_name_length);
    
    return strncmp(entry1->station_name, entry2->station_name, length);
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
    memset(all_entries, 0, entries_size * sizeof(HashMapEntry));
    
    s64 chunk_size = file_size / n_threads;
    char *file_end_address = file + file_size;
    char *file_ptr = file;
    for (int i = 0; i < n_threads; ++i) {
        thread_params[i].tid = (u8)i;
        thread_params[i].file_ptr_start = file_ptr;
        char *next = next_character(file_ptr + chunk_size, '\n');
        thread_params[i].file_ptr_end = (i == n_threads - 1) ? file_end_address : next;
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
                
                HashMapEntry *result_entry;
                for (;;) {
                    result_entry = result + entry_hash;
                    if (result_entry->station_name && !str_equals(result_entry->station_name, result_entry->station_name_length,
                                                                  entry.station_name, entry.station_name_length))
                    {
                        ++entry_hash;
                    }
                    else
                    {
                        break;
                    }
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
    

    qsort(result, MAX_STATIONS, sizeof(HashMapEntry), cmp);


    printf("{\n");
    for (int i = 0; i < MAX_STATIONS; ++i) {
        HashMapEntry entry = result[i];
        if (entry.station_name) {
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
