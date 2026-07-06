#include "ducknng_wire.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/*
 * Wire header layout (little-endian), pinned at compile time so the hard-coded
 * encode/decode offsets below cannot silently drift from the declared header
 * length or the field widths they assume:
 *   [0]   version   u8
 *   [1]   type      u8
 *   [2]   flags     u32   (offset 2)
 *   [6]   name_len  u32   (offset 2 + 4)
 *   [10]  error_len u32   (offset 6 + 4)
 *   [14]  payload_len u64 (offset 10 + 4); header ends at 14 + 8 = 22
 */
_Static_assert(DUCKNNG_WIRE_HEADER_LEN == 1u + 1u + 4u + 4u + 4u + 8u,
    "ducknng wire header length must match its u8+u8+u32+u32+u32+u64 field layout");
_Static_assert(sizeof(uint8_t) == 1 && sizeof(uint32_t) == 4 && sizeof(uint64_t) == 8,
    "ducknng wire encoding assumes 8/32/64-bit fixed-width integers");
_Static_assert(DUCKNNG_WIRE_VERSION <= 0xffu &&
    DUCKNNG_RPC_EVENT <= 0xff && DUCKNNG_STATUS_DISABLED <= 0xff,
    "ducknng wire version/type/status ids must each fit in their single header byte");
_Static_assert(DUCKNNG_RPC_FLAG_PAYLOAD_QUACK_BATCH <= 0xffffffffu,
    "ducknng rpc flags must fit in the u32 flags field");

int ducknng_decode_frame_bytes(const uint8_t *data, size_t len, ducknng_frame *out) {
    uint32_t name_len;
    uint32_t flags;
    uint32_t error_len;
    uint64_t payload_len;
    if (!out || !data || len < DUCKNNG_WIRE_HEADER_LEN) return -1;
    memset(out, 0, sizeof(*out));
    out->version = data[0];
    out->type = data[1];
    if (out->version != DUCKNNG_WIRE_VERSION) return -1;
    flags = ducknng_le32_read(data + 2);
    name_len = ducknng_le32_read(data + 6);
    error_len = ducknng_le32_read(data + 10);
    payload_len = ducknng_le64_read(data + 14);
    if (name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return -1;
    if (len < DUCKNNG_WIRE_HEADER_LEN + (size_t)name_len + (size_t)error_len) return -1;
    if ((uint64_t)(len - DUCKNNG_WIRE_HEADER_LEN - (size_t)name_len - (size_t)error_len) < payload_len) return -1;
    if (out->type == DUCKNNG_RPC_CALL && error_len != 0) return -1;
    out->flags = flags;
    out->name_len = name_len;
    out->name = data + DUCKNNG_WIRE_HEADER_LEN;
    out->error_len = error_len;
    out->error = out->name + name_len;
    out->payload_len = payload_len;
    out->payload = out->error + error_len;
    return 0;
}

int ducknng_decode_request(nng_msg *msg, ducknng_frame *out) {
    const uint8_t *data = (const uint8_t *)nng_msg_body(msg);
    size_t len = nng_msg_len(msg);
    return ducknng_decode_frame_bytes(data, len, out);
}

int ducknng_frame_name_equals(const ducknng_frame *frame, const char *name) {
    size_t n;
    if (!frame || !name) return 0;
    n = strlen(name);
    return frame->name_len == (uint32_t)n && memcmp(frame->name, name, n) == 0;
}

nng_msg *ducknng_build_reply(uint8_t type, const char *name, uint32_t flags,
    const char *error, const void *payload, uint64_t payload_len) {
    nng_msg *msg = NULL;
    uint8_t hdr[DUCKNNG_WIRE_HEADER_LEN];
    uint32_t name_len = name ? (uint32_t)strlen(name) : 0;
    uint32_t error_len = error ? (uint32_t)strlen(error) : 0;
    if (name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return NULL;
    if (nng_msg_alloc(&msg, 0) != 0) return NULL;
    memset(hdr, 0, sizeof(hdr));
    hdr[0] = DUCKNNG_WIRE_VERSION;
    hdr[1] = type;
    ducknng_le32_write(hdr + 2, flags);
    ducknng_le32_write(hdr + 6, name_len);
    ducknng_le32_write(hdr + 10, error_len);
    ducknng_le64_write(hdr + 14, payload_len);
    if (nng_msg_append(msg, hdr, sizeof(hdr)) != 0) goto fail;
    if (name_len > 0 && nng_msg_append(msg, name, name_len) != 0) goto fail;
    if (error_len > 0 && nng_msg_append(msg, error, error_len) != 0) goto fail;
    if (payload_len > 0 && payload && nng_msg_append(msg, payload, (size_t)payload_len) != 0) goto fail;
    return msg;
fail:
    nng_msg_free(msg);
    return NULL;
}

nng_msg *ducknng_error_msg(const char *name, int32_t code, const char *message) {
    char errbuf[64];
    snprintf(errbuf, sizeof(errbuf), "%d", (int)code);
    (void)errbuf;
    return ducknng_build_reply(DUCKNNG_RPC_ERROR, name, 0,
        message ? message : "ducknng: unspecified error", NULL, 0);
}

int ducknng_upload_append_parse_prefix(const uint8_t *payload, size_t payload_len,
    uint64_t *out_session_id, const uint8_t **out_token, size_t *out_token_len,
    size_t *out_quack_offset) {
    uint16_t token_len;
    size_t token_off;
    if (out_session_id) *out_session_id = 0;
    if (out_token) *out_token = NULL;
    if (out_token_len) *out_token_len = 0;
    if (out_quack_offset) *out_quack_offset = 0;
    /* Fixed header: 8-byte session id + 2-byte token length. */
    if (!payload || payload_len < 10u) return -1;
    token_len = ducknng_le16_read(payload + 8);
    if (token_len == 0 || token_len > DUCKNNG_UPLOAD_TOKEN_MAX) return -1;
    token_off = 10u;
    /* token_off + token_len cannot overflow: token_off is 10 and token_len
     * is bounded by DUCKNNG_UPLOAD_TOKEN_MAX, both far below SIZE_MAX. */
    if (token_off + (size_t)token_len > payload_len) return -1;
    if (out_session_id) *out_session_id = ducknng_le64_read(payload);
    if (out_token) *out_token = payload + token_off;
    if (out_token_len) *out_token_len = (size_t)token_len;
    if (out_quack_offset) *out_quack_offset = token_off + (size_t)token_len;
    return 0;
}
