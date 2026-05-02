#include "ducknng_thread.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef _WIN32
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#else
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

DUCKDB_EXTENSION_EXTERN

char *ducknng_strdup(const char *s) {
    size_t n;
    char *out;
    if (!s) return NULL;
    n = strlen(s);
    out = (char *)duckdb_malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, s, n + 1);
    return out;
}

char *ducknng_make_temp_dir(const char *prefix) {
#ifndef _WIN32
    const char *base;
    const char *stem;
    size_t len;
    char *templ;
    base = getenv("TMPDIR");
    if (!base || !base[0]) base = "/tmp";
    stem = (prefix && prefix[0]) ? prefix : "ducknng-";
    len = strlen(base) + 1 + strlen(stem) + 6 + 1;
    templ = (char *)duckdb_malloc(len);
    if (!templ) return NULL;
    snprintf(templ, len, "%s/%sXXXXXX", base, stem);
    if (!mkdtemp(templ)) {
        duckdb_free(templ);
        return NULL;
    }
    return templ;
#else
    char temp_path[MAX_PATH];
    char file_path[MAX_PATH];
    const char *tag = "dng";
    UINT n;
    (void)prefix;
    n = GetTempPathA((DWORD)sizeof(temp_path), temp_path);
    if (n == 0 || n >= sizeof(temp_path)) return NULL;
    if (GetTempFileNameA(temp_path, tag, 0, file_path) == 0) return NULL;
    DeleteFileA(file_path);
    if (!CreateDirectoryA(file_path, NULL)) return NULL;
    return ducknng_strdup(file_path);
#endif
}

int ducknng_remove_file(const char *path) {
    if (!path || !path[0]) return -1;
#ifndef _WIN32
    return unlink(path);
#else
    return DeleteFileA(path) ? 0 : -1;
#endif
}

int ducknng_remove_dir(const char *path) {
    if (!path || !path[0]) return -1;
#ifndef _WIN32
    return rmdir(path);
#else
    return RemoveDirectoryA(path) ? 0 : -1;
#endif
}

uint64_t ducknng_now_ms(void) {
#ifndef _WIN32
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t)ts.tv_sec * 1000ULL) + (uint64_t)(ts.tv_nsec / 1000000ULL);
#else
    return (uint64_t)GetTickCount64();
#endif
}

void ducknng_sleep_ms(uint64_t ms) {
#ifndef _WIN32
    usleep((useconds_t)(ms * 1000ULL));
#else
    Sleep((DWORD)ms);
#endif
}

int ducknng_ascii_tolower_int(int c) {
    return (c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c;
}

int ducknng_ascii_ieq(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        if (ducknng_ascii_tolower_int((unsigned char)*a) !=
                ducknng_ascii_tolower_int((unsigned char)*b)) {
            return 0;
        }
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

int ducknng_ascii_istarts_with(const char *s, const char *prefix) {
    if (!s || !prefix) return 0;
    while (*prefix) {
        if (!*s || ducknng_ascii_tolower_int((unsigned char)*s) !=
                ducknng_ascii_tolower_int((unsigned char)*prefix)) {
            return 0;
        }
        s++;
        prefix++;
    }
    return 1;
}

int ducknng_ascii_iends_with(const char *s, const char *suffix) {
    size_t slen;
    size_t suffix_len;
    if (!s || !suffix) return 0;
    slen = strlen(s);
    suffix_len = strlen(suffix);
    if (suffix_len > slen) return 0;
    return ducknng_ascii_ieq(s + slen - suffix_len, suffix);
}

uint16_t ducknng_le16_read(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }
uint32_t ducknng_le32_read(const uint8_t *p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }
uint64_t ducknng_le64_read(const uint8_t *p) { return (uint64_t)ducknng_le32_read(p) | ((uint64_t)ducknng_le32_read(p + 4) << 32); }
void ducknng_le16_write(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v & 0xff); p[1] = (uint8_t)((v >> 8) & 0xff); }
void ducknng_le32_write(uint8_t *p, uint32_t v) { p[0] = (uint8_t)(v & 0xff); p[1] = (uint8_t)((v >> 8) & 0xff); p[2] = (uint8_t)((v >> 16) & 0xff); p[3] = (uint8_t)((v >> 24) & 0xff); }
void ducknng_le64_write(uint8_t *p, uint64_t v) { ducknng_le32_write(p, (uint32_t)(v & 0xffffffffu)); ducknng_le32_write(p + 4, (uint32_t)(v >> 32)); }

#ifdef _WIN32
typedef struct {
    void *(*fn)(void *);
    void *arg;
} ducknng_thread_start;

static DWORD WINAPI ducknng_thread_main(LPVOID arg) {
    ducknng_thread_start *start = (ducknng_thread_start *)arg;
    if (start) {
        void *(*fn)(void *) = start->fn;
        void *fn_arg = start->arg;
        duckdb_free(start);
        if (fn) fn(fn_arg);
    }
    return 0;
}
#endif

int ducknng_thread_create(ducknng_thread *thread, void *(*fn)(void *), void *arg) {
#ifndef _WIN32
    return pthread_create(thread, NULL, fn, arg);
#else
    ducknng_thread_start *start;
    HANDLE handle;
    if (!thread || !fn) return -1;
    start = (ducknng_thread_start *)duckdb_malloc(sizeof(*start));
    if (!start) return -1;
    start->fn = fn;
    start->arg = arg;
    handle = CreateThread(NULL, 0, ducknng_thread_main, start, 0, NULL);
    if (!handle) {
        duckdb_free(start);
        return -1;
    }
    *thread = (ducknng_thread)handle;
    return 0;
#endif
}
void ducknng_thread_join(ducknng_thread thread) {
#ifndef _WIN32
    pthread_join(thread, NULL);
#else
    HANDLE handle = (HANDLE)thread;
    if (!handle) return;
    WaitForSingleObject(handle, INFINITE);
    CloseHandle(handle);
#endif
}
int ducknng_mutex_init(ducknng_mutex *mu) {
#ifndef _WIN32
    return pthread_mutex_init(mu, NULL);
#else
    CRITICAL_SECTION *cs;
    if (!mu) return -1;
    cs = (CRITICAL_SECTION *)duckdb_malloc(sizeof(*cs));
    if (!cs) return -1;
    InitializeCriticalSection(cs);
    *mu = (ducknng_mutex)cs;
    return 0;
#endif
}
void ducknng_mutex_lock(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_lock(mu);
#else
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cs) return;
    EnterCriticalSection(cs);
#endif
}
void ducknng_mutex_unlock(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_unlock(mu);
#else
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cs) return;
    LeaveCriticalSection(cs);
#endif
}
void ducknng_mutex_destroy(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_destroy(mu);
#else
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cs) return;
    DeleteCriticalSection(cs);
    duckdb_free(cs);
    *mu = NULL;
#endif
}
int ducknng_cond_init(ducknng_cond *cv) {
#ifndef _WIN32
    return pthread_cond_init(cv, NULL);
#else
    CONDITION_VARIABLE *cond;
    if (!cv) return -1;
    cond = (CONDITION_VARIABLE *)duckdb_malloc(sizeof(*cond));
    if (!cond) return -1;
    InitializeConditionVariable(cond);
    *cv = (ducknng_cond)cond;
    return 0;
#endif
}
void ducknng_cond_wait(ducknng_cond *cv, ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_cond_wait(cv, mu);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cond || !cs) return;
    SleepConditionVariableCS(cond, cs, INFINITE);
#endif
}
int ducknng_cond_timedwait_ms(ducknng_cond *cv, ducknng_mutex *mu, uint64_t timeout_ms) {
#ifndef _WIN32
    struct timespec ts;
    uint64_t ns_total;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return -1;
    ns_total = (uint64_t)ts.tv_nsec + (timeout_ms % 1000ULL) * 1000000ULL;
    ts.tv_sec += (time_t)(timeout_ms / 1000ULL) + (time_t)(ns_total / 1000000000ULL);
    ts.tv_nsec = (long)(ns_total % 1000000000ULL);
    if (pthread_cond_timedwait(cv, mu, &ts) == ETIMEDOUT) return 1;
    return 0;
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    DWORD rc;
    DWORD wait_ms = timeout_ms > (uint64_t)INFINITE - 1 ? (INFINITE - 1) : (DWORD)timeout_ms;
    if (!cond || !cs) return -1;
    rc = SleepConditionVariableCS(cond, cs, wait_ms);
    if (rc) return 0;
    return GetLastError() == ERROR_TIMEOUT ? 1 : -1;
#endif
}
void ducknng_cond_signal(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_signal(cv);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    if (!cond) return;
    WakeConditionVariable(cond);
#endif
}
void ducknng_cond_broadcast(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_broadcast(cv);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    if (!cond) return;
    WakeAllConditionVariable(cond);
#endif
}
void ducknng_cond_destroy(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_destroy(cv);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    if (!cond) return;
    duckdb_free(cond);
    *cv = NULL;
#endif
}
