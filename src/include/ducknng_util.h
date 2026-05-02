#pragma once
#include "duckdb_extension.h"
#include <stddef.h>
#include <stdint.h>

char *ducknng_strdup(const char *s);
char *ducknng_make_temp_dir(const char *prefix);
int ducknng_remove_file(const char *path);
int ducknng_remove_dir(const char *path);
uint64_t ducknng_now_ms(void);
void ducknng_sleep_ms(uint64_t ms);
int ducknng_ascii_tolower_int(int c);
int ducknng_ascii_ieq(const char *a, const char *b);
int ducknng_ascii_istarts_with(const char *s, const char *prefix);
int ducknng_ascii_iends_with(const char *s, const char *suffix);
uint16_t ducknng_le16_read(const uint8_t *p);
uint32_t ducknng_le32_read(const uint8_t *p);
uint64_t ducknng_le64_read(const uint8_t *p);
void ducknng_le16_write(uint8_t *p, uint16_t v);
void ducknng_le32_write(uint8_t *p, uint32_t v);
void ducknng_le64_write(uint8_t *p, uint64_t v);
