#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <windows.h>
#include <mmsystem.h>
#include <shellapi.h>
#include <process.h>
#include <wchar.h>
#include <wctype.h>

#ifdef _MSC_VER
#pragma comment(lib, "winmm.lib")
#endif

#define MAX_MEASURES 2000
#define MAX_ID62 (62 * 62)
#define EPS 1e-9

typedef struct {
    int channel;
    double pos;
    char token[3];
    int token_id;
} RawEvent;

typedef struct {
    RawEvent ev;
    int orig_index;
} RawEventIdx;

typedef struct {
    RawEvent *items;
    size_t len;
    size_t cap;
} RawEventVec;

typedef struct {
    int lane;
    double pos;
    int measure;
    int is_ln;
    int id;
    int wav_id;
} NoteEvent;

typedef struct {
    NoteEvent *items;
    size_t len;
    size_t cap;
} NoteEventVec;

typedef struct {
    double pos;
    int kind;
    double value;
    int note_index;
} MeasureEvent;

typedef struct {
    MeasureEvent *items;
    size_t len;
    size_t cap;
} MeasureEventVec;

typedef struct {
    int lane;
    int time_ms;
    int end_ms;
    int is_ln;
    int wav_id;
} OsuNote;

typedef struct {
    OsuNote *items;
    size_t len;
    size_t cap;
} OsuNoteVec;

typedef struct {
    int time_ms;
    int wav_id;
} AutoSample;

typedef struct {
    AutoSample *items;
    size_t len;
    size_t cap;
} AutoSampleVec;


typedef struct {
    int time_ms;
    double bpm;
} TimingPoint;

typedef struct {
    TimingPoint *items;
    size_t len;
    size_t cap;
} TimingPointVec;

typedef struct {
    char title[256];
    char artist[256];
    char genre[256];
    char subtitle[256];
    char difficulty[64];
    int rank_bms;
    double initial_bpm;
    double measure_len[MAX_MEASURES];
    int max_measure_seen;
    double bpm_ext[MAX_ID62];
    int bpm_ext_set[MAX_ID62];
    double stop_ext[MAX_ID62];
    int stop_ext_set[MAX_ID62];
    char *wav_defs[MAX_ID62];
    int token_base;
} BmsContext;

typedef struct {
    char **items;
    size_t len;
    size_t cap;
} StringVec;

typedef struct {
    char *src_path;
    char *arc_name;
} PackItem;

typedef struct {
    char **keys;
    size_t cap;
    size_t len;
} ArcSet;

typedef struct {
    PackItem *items;
    size_t len;
    size_t cap;
    ArcSet arcs;
} PackVec;

static volatile LONG g_bg_pick_seq = 0;

#if defined(_MSC_VER)
#define TLSVAR __declspec(thread)
#else
#define TLSVAR __thread
#endif


static void *xrealloc(void *ptr, size_t size) {
    void *out = realloc(ptr, size);
    if (!out) {
        fprintf(stderr, "Out of memory\n");
        exit(1);
    }
    return out;
}

static void raw_push(RawEventVec *v, RawEvent e) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 256;
        v->items = (RawEvent *)xrealloc(v->items, v->cap * sizeof(RawEvent));
    }
    v->items[v->len++] = e;
}

static void note_push(NoteEventVec *v, NoteEvent e) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 256;
        v->items = (NoteEvent *)xrealloc(v->items, v->cap * sizeof(NoteEvent));
    }
    v->items[v->len++] = e;
}

static void mev_push(MeasureEventVec *v, MeasureEvent e) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 32;
        v->items = (MeasureEvent *)xrealloc(v->items, v->cap * sizeof(MeasureEvent));
    }
    v->items[v->len++] = e;
}

static void osu_push(OsuNoteVec *v, OsuNote e) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 512;
        v->items = (OsuNote *)xrealloc(v->items, v->cap * sizeof(OsuNote));
    }
    v->items[v->len++] = e;
}

static void tp_push(TimingPointVec *v, TimingPoint e) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 64;
        v->items = (TimingPoint *)xrealloc(v->items, v->cap * sizeof(TimingPoint));
    }
    v->items[v->len++] = e;
}

static void auto_push(AutoSampleVec *v, AutoSample e) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 256;
        v->items = (AutoSample *)xrealloc(v->items, v->cap * sizeof(AutoSample));
    }
    v->items[v->len++] = e;
}


static char *xstrdup(const char *s) {
    size_t n = strlen(s);
    char *out = (char *)xrealloc(NULL, n + 1);
    memcpy(out, s, n + 1);
    return out;
}

static void str_push(StringVec *v, const char *s) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 16;
        v->items = (char **)xrealloc(v->items, v->cap * sizeof(char *));
    }
    v->items[v->len++] = xstrdup(s);
}

static void str_free(StringVec *v) {
    for (size_t i = 0; i < v->len; i++) free(v->items[i]);
    free(v->items);
    v->items = NULL;
    v->len = v->cap = 0;
}

static void pack_push(PackVec *v, const char *src, const char *arc) {
    if (v->len == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 32;
        v->items = (PackItem *)xrealloc(v->items, v->cap * sizeof(PackItem));
    }
    v->items[v->len].src_path = xstrdup(src);
    v->items[v->len].arc_name = xstrdup(arc);
    v->len++;
}

static uint32_t hash_ci(const char *s) {
    uint32_t h = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        h ^= (uint32_t)tolower(*p);
        h *= 16777619u;
    }
    return h;
}

static void arcset_rehash(ArcSet *s, size_t new_cap) {
    char **old = s->keys;
    size_t old_cap = s->cap;
    s->keys = (char **)xrealloc(NULL, new_cap * sizeof(char *));
    for (size_t i = 0; i < new_cap; i++) s->keys[i] = NULL;
    s->cap = new_cap;
    s->len = 0;
    for (size_t i = 0; i < old_cap; i++) {
        if (!old || !old[i]) continue;
        uint32_t h = hash_ci(old[i]);
        size_t m = new_cap - 1;
        for (size_t k = 0; k < new_cap; k++) {
            size_t pos = (h + (uint32_t)k) & m;
            if (!s->keys[pos]) {
                s->keys[pos] = old[i];
                s->len++;
                break;
            }
        }
    }
    free(old);
}

static int arcset_insert_if_new(ArcSet *s, const char *key) {
    if (s->cap == 0) arcset_rehash(s, 128);
    if ((s->len + 1) * 10 >= s->cap * 7) arcset_rehash(s, s->cap * 2);
    uint32_t h = hash_ci(key);
    size_t m = s->cap - 1;
    for (size_t k = 0; k < s->cap; k++) {
        size_t pos = (h + (uint32_t)k) & m;
        if (!s->keys[pos]) {
            s->keys[pos] = xstrdup(key);
            s->len++;
            return 1;
        }
        if (_stricmp(s->keys[pos], key) == 0) return 0;
    }
    return 0;
}

static void pack_push_unique(PackVec *v, const char *src, const char *arc) {
    if (arcset_insert_if_new(&v->arcs, arc)) pack_push(v, src, arc);
}

static void pack_free(PackVec *v) {
    for (size_t i = 0; i < v->len; i++) {
        free(v->items[i].src_path);
        free(v->items[i].arc_name);
    }
    free(v->items);
    if (v->arcs.keys) {
        for (size_t i = 0; i < v->arcs.cap; i++) free(v->arcs.keys[i]);
        free(v->arcs.keys);
    }
    v->items = NULL;
    v->len = v->cap = 0;
    v->arcs.keys = NULL;
    v->arcs.cap = v->arcs.len = 0;
}

static int cmp_note(const void *a, const void *b) {
    const NoteEvent *x = (const NoteEvent *)a;
    const NoteEvent *y = (const NoteEvent *)b;
    if (x->measure != y->measure) return x->measure - y->measure;
    if (fabs(x->pos - y->pos) > EPS) return (x->pos < y->pos) ? -1 : 1;
    if (x->lane != y->lane) return x->lane - y->lane;
    return x->is_ln - y->is_ln;
}

static int cmp_mev(const void *a, const void *b) {
    const MeasureEvent *x = (const MeasureEvent *)a;
    const MeasureEvent *y = (const MeasureEvent *)b;
    if (fabs(x->pos - y->pos) > EPS) return (x->pos < y->pos) ? -1 : 1;
    return x->kind - y->kind;
}

static int cmp_osu(const void *a, const void *b) {
    const OsuNote *x = (const OsuNote *)a;
    const OsuNote *y = (const OsuNote *)b;
    if (x->time_ms != y->time_ms) return x->time_ms - y->time_ms;
    return x->lane - y->lane;
}

static int cmp_tp(const void *a, const void *b) {
    const TimingPoint *x = (const TimingPoint *)a;
    const TimingPoint *y = (const TimingPoint *)b;
    if (x->time_ms != y->time_ms) return x->time_ms - y->time_ms;
    if (fabs(x->bpm - y->bpm) < 1e-7) return 0;
    return (x->bpm < y->bpm) ? -1 : 1;
}

static int cmp_raw_measure_pos(const void *a, const void *b) {
    const RawEvent *x = (const RawEvent *)a;
    const RawEvent *y = (const RawEvent *)b;
    int mx = x->channel / 1000;
    int my = y->channel / 1000;
    if (mx != my) return mx - my;
    if (fabs(x->pos - y->pos) > EPS) return (x->pos < y->pos) ? -1 : 1;
    return (x->channel % 1000) - (y->channel % 1000);
}

static int cmp_rawidx_measure_pos(const void *a, const void *b) {
    const RawEventIdx *x = (const RawEventIdx *)a;
    const RawEventIdx *y = (const RawEventIdx *)b;
    int mx = x->ev.channel / 1000;
    int my = y->ev.channel / 1000;
    if (mx != my) return mx - my;
    if (fabs(x->ev.pos - y->ev.pos) > EPS) return (x->ev.pos < y->ev.pos) ? -1 : 1;
    return (x->ev.channel % 1000) - (y->ev.channel % 1000);
}

static int cmp_auto(const void *a, const void *b) {
    const AutoSample *x = (const AutoSample *)a;
    const AutoSample *y = (const AutoSample *)b;
    if (x->time_ms != y->time_ms) return x->time_ms - y->time_ms;
    return x->wav_id - y->wav_id;
}


static void trim(char *s) {
    char *p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1])) {
        s[n - 1] = '\0';
        n--;
    }
}

static int decode36char(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'Z') return 10 + (c - 'A');
    if (c >= 'a' && c <= 'z') return 10 + (c - 'a');
    return -1;
}

static int decode62char(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'Z') return 10 + (c - 'A');
    if (c >= 'a' && c <= 'z') return 36 + (c - 'a');
    return -1;
}

static void normalize_token2(char t[3], int base) {
    if (base == 36) {
        t[0] = (char)toupper((unsigned char)t[0]);
        t[1] = (char)toupper((unsigned char)t[1]);
    }
}

static int decode_token2(const char t[3], int base) {
    int a = (base == 62) ? decode62char(t[0]) : decode36char(t[0]);
    int b = (base == 62) ? decode62char(t[1]) : decode36char(t[1]);
    if (a < 0 || b < 0) return -1;
    return a * base + b;
}

static int token_is_zero(const char t[3]) {
    return t[0] == '0' && t[1] == '0';
}

static int channel_to_lane(int channel, int *is_ln, int use_iidx_layout, int cut_scratch) {
    int hi = channel / 10;
    int lo = channel % 10;
    *is_ln = 0;
    if (lo < 1 || lo > 9) return -1;
    if (hi == 1) {
        if (use_iidx_layout) {
            if (cut_scratch) {
                static const int map1p_cut[10] = {-1, 0, 1, 2, 3, 4, -1, -1, 5, 6};
                return map1p_cut[lo];
            } else {
                static const int map1p[10] = {-1, 1, 2, 3, 4, 5, 0, -1, 6, 7};
                return map1p[lo];
            }
        }
        return lo - 1;
    }
    if (hi == 2) {
        return (use_iidx_layout ? 8 : 9) + (lo - 1);
    }
    if (hi == 5) {
        *is_ln = 1;
        if (use_iidx_layout) {
            if (cut_scratch) {
                static const int map1p_ln_cut[10] = {-1, 0, 1, 2, 3, 4, -1, -1, 5, 6};
                return map1p_ln_cut[lo];
            } else {
                static const int map1p_ln[10] = {-1, 1, 2, 3, 4, 5, 0, -1, 6, 7};
                return map1p_ln[lo];
            }
        }
        return lo - 1;
    }
    if (hi == 6) {
        *is_ln = 1;
        return (use_iidx_layout ? 8 : 9) + (lo - 1);
    }
    return -1;
}

static const char *path_basename(const char *p) {
    const char *s1 = strrchr(p, '/');
    const char *s2 = strrchr(p, '\\');
    if (s1 && s2) return (s1 > s2) ? (s1 + 1) : (s2 + 1);
    if (s1) return s1 + 1;
    if (s2) return s2 + 1;
    return p;
}

static void path_dirname(const char *path, char *out, size_t out_sz) {
    snprintf(out, out_sz, "%s", path);
    char *s1 = strrchr(out, '/');
    char *s2 = strrchr(out, '\\');
    char *s = NULL;
    if (s1 && s2) s = (s1 > s2) ? s1 : s2;
    else if (s1) s = s1;
    else if (s2) s = s2;
    if (!s) {
        snprintf(out, out_sz, ".");
        return;
    }
    *s = '\0';
}

static void join_path(char *out, size_t out_sz, const char *dir, const char *name) {
    if (!dir || !dir[0] || strcmp(dir, ".") == 0) {
        snprintf(out, out_sz, "%s", name);
        return;
    }
    size_t n = strlen(dir);
    if (dir[n - 1] == '\\' || dir[n - 1] == '/') snprintf(out, out_sz, "%s%s", dir, name);
    else snprintf(out, out_sz, "%s\\%s", dir, name);
}

static int file_exists(const char *path) {
    DWORD a = GetFileAttributesA(path);
    return a != INVALID_FILE_ATTRIBUTES && !(a & FILE_ATTRIBUTE_DIRECTORY);
}

static int dir_exists(const char *path) {
    DWORD a = GetFileAttributesA(path);
    return a != INVALID_FILE_ATTRIBUTES && (a & FILE_ATTRIBUTE_DIRECTORY);
}

static int is_abs_path(const char *p) {
    if (!p || !p[0]) return 0;
    if ((isalpha((unsigned char)p[0]) && p[1] == ':' && (p[2] == '\\' || p[2] == '/'))) return 1;
    if ((p[0] == '\\' && p[1] == '\\') || (p[0] == '/' && p[1] == '/')) return 1;
    return 0;
}

static int wdir_exists(const wchar_t *path) {
    DWORD a = GetFileAttributesW(path);
    return a != INVALID_FILE_ATTRIBUTES && (a & FILE_ATTRIBUTE_DIRECTORY);
}

static int is_abs_path_w(const wchar_t *p) {
    if (!p || !p[0]) return 0;
    if (iswalpha(p[0]) && p[1] == L':' && (p[2] == L'\\' || p[2] == L'/')) return 1;
    if ((p[0] == L'\\' && p[1] == L'\\') || (p[0] == L'/' && p[1] == L'/')) return 1;
    return 0;
}

static void wpath_dirname(const wchar_t *path, wchar_t *out, size_t out_sz) {
    wcsncpy(out, path, out_sz - 1);
    out[out_sz - 1] = L'\0';
    wchar_t *s1 = wcsrchr(out, L'/');
    wchar_t *s2 = wcsrchr(out, L'\\');
    wchar_t *s = NULL;
    if (s1 && s2) s = (s1 > s2) ? s1 : s2;
    else if (s1) s = s1;
    else if (s2) s = s2;
    if (!s) {
        wcsncpy(out, L".", out_sz - 1);
        out[out_sz - 1] = L'\0';
        return;
    }
    *s = L'\0';
}

static void join_path_w(wchar_t *out, size_t out_sz, const wchar_t *dir, const wchar_t *name) {
    if (!dir || !dir[0] || wcscmp(dir, L".") == 0) {
        _snwprintf(out, out_sz - 1, L"%ls", name);
        out[out_sz - 1] = L'\0';
        return;
    }
    size_t n = wcslen(dir);
    if (dir[n - 1] == L'\\' || dir[n - 1] == L'/') _snwprintf(out, out_sz - 1, L"%ls%ls", dir, name);
    else _snwprintf(out, out_sz - 1, L"%ls\\%ls", dir, name);
    out[out_sz - 1] = L'\0';
}

static int wide_to_ansi(const wchar_t *src, char *out, size_t out_sz) {
    if (!src || !out || out_sz == 0) return 0;
    BOOL used_default = FALSE;
    int n = WideCharToMultiByte(CP_ACP, WC_NO_BEST_FIT_CHARS, src, -1, out, (int)out_sz, NULL, &used_default);
    if (n <= 0 || (size_t)n >= out_sz) return 0;
    if (used_default) return 0;
    return 1;
}

static int wide_dir_to_ansi_path(const wchar_t *wpath, char *out, size_t out_sz) {
    if (wide_to_ansi(wpath, out, out_sz)) return 1;
    wchar_t shortp[4096];
    DWORD n = GetShortPathNameW(wpath, shortp, (DWORD)(sizeof(shortp) / sizeof(shortp[0])));
    if (n > 0 && n < (DWORD)(sizeof(shortp) / sizeof(shortp[0]))) {
        if (wide_to_ansi(shortp, out, out_sz)) return 1;
    }
    return 0;
}

static int ansi_to_wide(const char *src, wchar_t *out, size_t out_sz) {
    if (!src || !out || out_sz == 0) return 0;
    int n = MultiByteToWideChar(CP_ACP, 0, src, -1, out, (int)out_sz);
    if (n <= 0 || (size_t)n > out_sz) return 0;
    return 1;
}

static int resolve_input_dir_w(const wchar_t *input, const wchar_t *exe_dir_w, char *out, size_t out_sz) {
    if (!input || !input[0]) return 0;
    if (is_abs_path_w(input)) {
        if (!wdir_exists(input)) return 0;
        return wide_dir_to_ansi_path(input, out, out_sz);
    }
    if (wdir_exists(input)) {
        return wide_dir_to_ansi_path(input, out, out_sz);
    }
    {
        wchar_t p1[4096];
        join_path_w(p1, sizeof(p1) / sizeof(p1[0]), exe_dir_w, input);
        if (wdir_exists(p1)) return wide_dir_to_ansi_path(p1, out, out_sz);
    }
    {
        wchar_t parent[4096];
        wchar_t p2[4096];
        wpath_dirname(exe_dir_w, parent, sizeof(parent) / sizeof(parent[0]));
        join_path_w(p2, sizeof(p2) / sizeof(p2[0]), parent, input);
        if (wdir_exists(p2)) return wide_dir_to_ansi_path(p2, out, out_sz);
    }
    return 0;
}

static int resolve_input_dir(const char *input, const char *exe_dir, char *out, size_t out_sz) {
    if (!input || !input[0]) return 0;
    if (is_abs_path(input)) {
        if (!dir_exists(input)) return 0;
        snprintf(out, out_sz, "%s", input);
        return 1;
    }
    if (dir_exists(input)) {
        snprintf(out, out_sz, "%s", input);
        return 1;
    }
    {
        char p1[4096];
        join_path(p1, sizeof(p1), exe_dir, input);
        if (dir_exists(p1)) {
            snprintf(out, out_sz, "%s", p1);
            return 1;
        }
    }
    {
        char parent[4096];
        char p2[4096];
        path_dirname(exe_dir, parent, sizeof(parent));
        join_path(p2, sizeof(p2), parent, input);
        if (dir_exists(p2)) {
            snprintf(out, out_sz, "%s", p2);
            return 1;
        }
    }
    return 0;
}

static int has_audio_ext(const char *name) {
    const char *dot = strrchr(name, '.');
    if (!dot) return 0;
    return _stricmp(dot, ".wav") == 0 || _stricmp(dot, ".ogg") == 0 || _stricmp(dot, ".mp3") == 0 || _stricmp(dot, ".flac") == 0;
}

static int has_image_ext(const char *name) {
    const char *dot = strrchr(name, '.');
    if (!dot) return 0;
    return _stricmp(dot, ".png") == 0 || _stricmp(dot, ".jpg") == 0 || _stricmp(dot, ".jpeg") == 0 ||
           _stricmp(dot, ".bmp") == 0 || _stricmp(dot, ".webp") == 0;
}

static uint32_t hash_path_fnv1a(const char *s) {
    uint32_t h = 2166136261u;
    if (!s) return h;
    while (*s) {
        unsigned char c = (unsigned char)*s++;
        h ^= (uint32_t)c;
        h *= 16777619u;
    }
    return h;
}

static int has_video_ext(const char *name) {
    const char *dot = strrchr(name, '.');
    if (!dot) return 0;
    return _stricmp(dot, ".mp4") == 0 || _stricmp(dot, ".avi") == 0 || _stricmp(dot, ".wmv") == 0 ||
           _stricmp(dot, ".mpg") == 0 || _stricmp(dot, ".mpeg") == 0 || _stricmp(dot, ".mkv") == 0 ||
           _stricmp(dot, ".webm") == 0 || _stricmp(dot, ".mov") == 0 || _stricmp(dot, ".flv") == 0 ||
           _stricmp(dot, ".m4v") == 0 || _stricmp(dot, ".ts") == 0 || _stricmp(dot, ".mts") == 0;
}

static int query_media_length_ms_mci(const char *full_path, int *out_ms) {
    char alias[64];
    char cmd[8192];
    char ret[128];
    MCIERROR err;
    snprintf(alias, sizeof(alias), "vid%lu", (unsigned long)GetTickCount());
    snprintf(cmd, sizeof(cmd), "open \"%s\" alias %s", full_path, alias);
    err = mciSendStringA(cmd, NULL, 0, NULL);
    if (err != 0) return 0;
    snprintf(cmd, sizeof(cmd), "set %s time format milliseconds", alias);
    mciSendStringA(cmd, NULL, 0, NULL);
    snprintf(cmd, sizeof(cmd), "status %s length", alias);
    err = mciSendStringA(cmd, ret, (UINT)sizeof(ret), NULL);
    snprintf(cmd, sizeof(cmd), "close %s", alias);
    mciSendStringA(cmd, NULL, 0, NULL);
    if (err != 0) return 0;
    *out_ms = atoi(ret);
    return *out_ms > 0;
}

static int find_video_over_1min(const char *map_dir, char *out_name, size_t out_sz) {
    char pat[4096];
    WIN32_FIND_DATAA fd;
    HANDLE h;
    int best_ms = 0;
    out_name[0] = '\0';
    snprintf(pat, sizeof(pat), "%s\\*", map_dir);
    h = FindFirstFileA(pat, &fd);
    if (h == INVALID_HANDLE_VALUE) return 0;
    do {
        char full[4096];
        int ms = 0;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) continue;
        if (!has_video_ext(fd.cFileName)) continue;
        join_path(full, sizeof(full), map_dir, fd.cFileName);
        if (!query_media_length_ms_mci(full, &ms)) continue;
        if (ms < 60000) continue;
        if (ms > best_ms) {
            best_ms = ms;
            snprintf(out_name, out_sz, "%s", fd.cFileName);
        }
    } while (FindNextFileA(h, &fd));
    FindClose(h);
    return out_name[0] != '\0';
}

static int starts_with_icase(const char *s, const char *prefix) {
    while (*prefix) {
        if (!*s) return 0;
        if (toupper((unsigned char)*s) != toupper((unsigned char)*prefix)) return 0;
        s++;
        prefix++;
    }
    return 1;
}

static void copy_field(char *dst, size_t dst_sz, const char *src) {
    if (dst_sz == 0) return;
    if (!src) src = "";
    size_t n = strlen(src);
    if (n >= dst_sz) n = dst_sz - 1;
    if (n > 0) memcpy(dst, src, n);
    dst[n] = '\0';
    trim(dst);
}

static int read_bms_line(FILE *fp, char **buf, size_t *cap) {
    if (!fp || !buf || !cap) return 0;
    if (!*buf || *cap < 256) {
        *cap = 1024;
        *buf = (char *)xrealloc(*buf, *cap);
    }
    (*buf)[0] = '\0';
    size_t len = 0;
    for (;;) {
        char chunk[4096];
        if (!fgets(chunk, sizeof(chunk), fp)) break;
        size_t clen = strlen(chunk);
        if (len + clen + 1 > *cap) {
            size_t ncap = *cap;
            while (len + clen + 1 > ncap) ncap *= 2;
            *cap = ncap;
            *buf = (char *)xrealloc(*buf, *cap);
        }
        memcpy(*buf + len, chunk, clen);
        len += clen;
        (*buf)[len] = '\0';
        if (clen > 0 && chunk[clen - 1] == '\n') break;
    }
    if (len == 0 && feof(fp)) return 0;
    while (len > 0 && ((*buf)[len - 1] == '\n' || (*buf)[len - 1] == '\r')) {
        (*buf)[--len] = '\0';
    }
    return 1;
}

static void init_context(BmsContext *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    for (int i = 0; i < MAX_MEASURES; i++) ctx->measure_len[i] = 1.0;
    ctx->initial_bpm = 130.0;
    ctx->token_base = 36;
    ctx->difficulty[0] = '\0';
    ctx->rank_bms = -1;
}

static int is_sp_chart_file(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return 0;
    int player = -1;
    int has11_19 = 0;
    int has21_29 = 0;
    char *line = NULL;
    size_t cap = 0;
    while (read_bms_line(fp, &line, &cap)) {
        trim(line);
        if (line[0] == '\0') continue;
        if (line[0] != '#') continue;
        if (starts_with_icase(line, "#PLAYER")) {
            char *v = line + 7;
            while (*v && isspace((unsigned char)*v)) v++;
            if (*v) {
                int p = atoi(v);
                if (p == 1 || p == 2 || p == 3) player = p;
            }
            continue;
        }
        if (strlen(line) >= 6 &&
            isdigit((unsigned char)line[1]) &&
            isdigit((unsigned char)line[2]) &&
            isdigit((unsigned char)line[3]) &&
            isdigit((unsigned char)line[4]) &&
            isdigit((unsigned char)line[5])) {
            int ch = (line[4] - '0') * 10 + (line[5] - '0');
            if (ch >= 11 && ch <= 19) has11_19 = 1;
            if (ch >= 21 && ch <= 29) has21_29 = 1;
        }
    }
    free(line);
    fclose(fp);
    if (player == 1) return 1;
    if (player == 2 || player == 3) return 0;
    return has11_19 && !has21_29;
}

static double od_from_bms_rank_fixed(int rank_bms) {
    switch (rank_bms) {
        case 0: return 9.9;
        case 1: return 8.1;
        case 2: return 7.6;
        case 3: return 6.9;
        case 4: return 4.9;
        default: return 8.0;
    }
}

static int parse_bms_file(const char *path, BmsContext *ctx, RawEventVec *raw_events) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "Cannot open %s: %s\n", path, strerror(errno));
        return 0;
    }

    char *line = NULL;
    size_t line_cap = 0;
    int line_no = 0;
    int random_vals[64];
    int random_top = -1;
    int skip_stack[64];
    int skip_top = 0;
    while (read_bms_line(fp, &line, &line_cap)) {
        line_no++;
        if (line_no == 1 && (unsigned char)line[0] == 0xEF && (unsigned char)line[1] == 0xBB && (unsigned char)line[2] == 0xBF) {
            memmove(line, line + 3, strlen(line + 3) + 1);
        }
        trim(line);
        if (line[0] == '\0') continue;
        if (line[0] == ';' || (line[0] == '/' && line[1] == '/')) continue;
        if (line[0] != '#') continue;

        if (starts_with_icase(line, "#RANDOM")) {
            if (random_top + 1 < 64) {
                int r = atoi(line + 7);
                if (r < 1) r = 1;
                random_vals[++random_top] = (rand() % r) + 1;
            }
            continue;
        }
        if (starts_with_icase(line, "#IF")) {
            if (skip_top < 64) {
                if (random_top >= 0) {
                    int need = atoi(line + 3);
                    skip_stack[skip_top++] = (random_vals[random_top] != need);
                } else {
                    skip_stack[skip_top++] = 1;
                }
            }
            continue;
        }
        if (starts_with_icase(line, "#ELSE")) {
            if (skip_top > 0) {
                skip_stack[skip_top - 1] = !skip_stack[skip_top - 1];
            }
            continue;
        }
        if (starts_with_icase(line, "#ENDIF")) {
            if (skip_top > 0) skip_top--;
            continue;
        }
        if (starts_with_icase(line, "#ENDRANDOM")) {
            if (random_top >= 0) random_top--;
            continue;
        }
        {
            int ignore = 0;
            for (int i = 0; i < skip_top; i++) {
                if (skip_stack[i]) {
                    ignore = 1;
                    break;
                }
            }
            if (ignore) continue;
        }

        if (starts_with_icase(line, "#TITLE")) {
            copy_field(ctx->title, sizeof(ctx->title), line + 6);
            continue;
        }
        if (starts_with_icase(line, "#ARTIST")) {
            copy_field(ctx->artist, sizeof(ctx->artist), line + 7);
            continue;
        }
        if (starts_with_icase(line, "#GENRE")) {
            copy_field(ctx->genre, sizeof(ctx->genre), line + 6);
            continue;
        }
        if (starts_with_icase(line, "#SUBTITLE")) {
            copy_field(ctx->subtitle, sizeof(ctx->subtitle), line + 9);
            continue;
        }
        if (starts_with_icase(line, "#PLAYLEVEL")) {
            copy_field(ctx->difficulty, sizeof(ctx->difficulty), line + 10);
            continue;
        }
        if (starts_with_icase(line, "#RANK")) {
            char *v = line + 5;
            while (*v && isspace((unsigned char)*v)) v++;
            if (*v) {
                int r = atoi(v);
                if (r >= 0 && r <= 4) ctx->rank_bms = r;
            }
            continue;
        }
        if (starts_with_icase(line, "#BPM ") || starts_with_icase(line, "#BPM\t")) {
            char *v = line + 4;
            while (*v && isspace((unsigned char)*v)) v++;
            if (*v) {
                double bpm = strtod(v, NULL);
                if (bpm > 0.0) ctx->initial_bpm = bpm;
            }
            continue;
        }

        if (starts_with_icase(line, "#BASE")) {
            char *v = line + 5;
            while (*v && isspace((unsigned char)*v)) v++;
            if (*v) {
                int b = atoi(v);
                if (b == 62) ctx->token_base = 62;
                else ctx->token_base = 36;
            }
            continue;
        }

        if (starts_with_icase(line, "#WAV") && strlen(line) >= 7) {
            char id[3] = {line[4], line[5], '\0'};
            normalize_token2(id, ctx->token_base);
            int idx = decode_token2(id, ctx->token_base);
            if (idx >= 0) {
                char *v = line + 6;
                trim(v);
                free(ctx->wav_defs[idx]);
                ctx->wav_defs[idx] = (char *)xrealloc(NULL, strlen(v) + 1);
                strcpy(ctx->wav_defs[idx], v);
            }
            continue;
        }
        if (starts_with_icase(line, "#BPM") && strlen(line) >= 7 && !isspace((unsigned char)line[4])) {
            char id[3] = {line[4], line[5], '\0'};
            normalize_token2(id, ctx->token_base);
            int idx = decode_token2(id, ctx->token_base);
            if (idx >= 0) {
                char *v = line + 6;
                trim(v);
                double bpm = strtod(v, NULL);
                if (bpm > 0.0) {
                    ctx->bpm_ext[idx] = bpm;
                    ctx->bpm_ext_set[idx] = 1;
                }
            }
            continue;
        }
        if (starts_with_icase(line, "#STOP") && strlen(line) >= 8) {
            char id[3] = {line[5], line[6], '\0'};
            normalize_token2(id, ctx->token_base);
            int idx = decode_token2(id, ctx->token_base);
            if (idx >= 0) {
                char *v = line + 7;
                trim(v);
                double stopv = strtod(v, NULL);
                if (stopv >= 0.0) {
                    ctx->stop_ext[idx] = stopv;
                    ctx->stop_ext_set[idx] = 1;
                }
            }
            continue;
        }

        if (strlen(line) >= 7 && line[0] == '#' && isdigit((unsigned char)line[1]) && isdigit((unsigned char)line[2]) && isdigit((unsigned char)line[3])) {
            if (!isalnum((unsigned char)line[4]) || !isalnum((unsigned char)line[5])) continue;
            if (line[6] != ':') continue;

            int measure = (line[1] - '0') * 100 + (line[2] - '0') * 10 + (line[3] - '0');
            int c1 = line[4];
            int c2 = line[5];
            int channel;
            if (isdigit((unsigned char)c1) && isdigit((unsigned char)c2)) {
                channel = (c1 - '0') * 10 + (c2 - '0');
            } else {
                channel = -1;
            }

            if (measure >= 0 && measure < MAX_MEASURES && measure > ctx->max_measure_seen) ctx->max_measure_seen = measure;

            const char *data = line + 7;
            if (channel == 2) {
                char tmp[128];
                strncpy(tmp, data, sizeof(tmp) - 1);
                tmp[sizeof(tmp) - 1] = '\0';
                double ratio = strtod(tmp, NULL);
                if (ratio > 0.0 && measure >= 0 && measure < MAX_MEASURES) {
                    ctx->measure_len[measure] = ratio;
                }
                continue;
            }

            size_t dlen = strlen(data);
            if (dlen < 2 || (dlen % 2) != 0) continue;
            int steps = (int)(dlen / 2);

            for (int i = 0; i < steps; i++) {
                char tk[3] = {data[i * 2], data[i * 2 + 1], '\0'};
                normalize_token2(tk, ctx->token_base);
                if (token_is_zero(tk)) continue;
                RawEvent ev;
                ev.channel = channel;
                ev.pos = (double)i / (double)steps;
                ev.token[0] = tk[0];
                ev.token[1] = tk[1];
                ev.token[2] = '\0';
                ev.token_id = decode_token2(tk, ctx->token_base);
                ev.channel += measure * 1000;
                raw_push(raw_events, ev);
            }
        }
    }

    free(line);
    fclose(fp);
    return 1;
}

static void split_raw_events(const RawEventVec *raw, NoteEventVec *notes, RawEventVec *timing, RawEventVec *bgm, int *max_lane, int *max_measure, int cut_scratch) {
    *max_lane = -1;
    *max_measure = 0;
    int has_16 = 0, has_17 = 0, has_18_19 = 0;
    for (size_t i = 0; i < raw->len; i++) {
        int ch = raw->items[i].channel % 1000;
        int hi = ch / 10;
        int lo = ch % 10;
        if (!(hi == 1 || hi == 5)) continue;
        if (lo == 6) has_16 = 1;
        if (lo == 7) has_17 = 1;
        if (lo == 8 || lo == 9) has_18_19 = 1;
    }
    int use_iidx_layout = (has_16 && has_18_19 && !has_17) ? 1 : 0;

    for (size_t i = 0; i < raw->len; i++) {
        RawEvent re = raw->items[i];
        int measure = re.channel / 1000;
        int ch = re.channel % 1000;
        if (measure > *max_measure) *max_measure = measure;

        int is_ln = 0;
        int lane = channel_to_lane(ch, &is_ln, use_iidx_layout, cut_scratch);
        if (lane >= 0) {
            NoteEvent ne;
            ne.lane = lane;
            ne.pos = re.pos;
            ne.measure = measure;
            ne.is_ln = is_ln;
            ne.id = (int)notes->len;
            ne.wav_id = re.token_id;
            note_push(notes, ne);
            if (lane > *max_lane) *max_lane = lane;
            continue;
        }

        if (cut_scratch && use_iidx_layout) {
            int hi = ch / 10;
            int lo = ch % 10;
            if ((hi == 1 || hi == 5 || hi == 2 || hi == 6) && lo == 6) {
                RawEvent b = re;
                b.channel = 1 + measure * 1000;
                raw_push(bgm, b);
                continue;
            }
        }

        if (ch == 3 || ch == 8 || ch == 9) {
            RawEvent t = re;
            t.channel = ch + measure * 1000;
            raw_push(timing, t);
        } else if (ch == 1) {
            RawEvent b = re;
            b.channel = 1 + measure * 1000;
            raw_push(bgm, b);
        }
    }
}

static double beats_to_ms(double beats, double bpm) {
    if (bpm <= 0.0) bpm = 130.0;
    return beats * (60000.0 / bpm);
}

static TLSVAR int *tls_note_time = NULL;
static TLSVAR size_t tls_note_time_cap = 0;
static TLSVAR int *tls_bgm_time = NULL;
static TLSVAR size_t tls_bgm_time_cap = 0;
static TLSVAR NoteEvent *tls_sorted_notes = NULL;
static TLSVAR size_t tls_sorted_notes_cap = 0;
static TLSVAR RawEvent *tls_timing_sorted = NULL;
static TLSVAR size_t tls_timing_sorted_cap = 0;
static TLSVAR RawEventIdx *tls_bgm_sorted = NULL;
static TLSVAR size_t tls_bgm_sorted_cap = 0;

static void *tls_ensure_buf(void *ptr, size_t *cap, size_t need, size_t elem_sz) {
    if (need == 0) need = 1;
    if (*cap < need) {
        size_t nc = *cap ? *cap : 256;
        while (nc < need) nc *= 2;
        ptr = xrealloc(ptr, nc * elem_sz);
        *cap = nc;
    }
    return ptr;
}

static void generate_timing_and_notes(
    const BmsContext *ctx,
    const NoteEventVec *notes,
    const RawEventVec *timing_raw,
    const RawEventVec *bgm_raw,
    int last_measure,
    OsuNoteVec *osu_notes,
    TimingPointVec *timing_points,
    AutoSampleVec *auto_samples) {

    int *note_time = NULL;
    int *bgm_time = NULL;
    NoteEvent *sorted_notes = NULL;
    RawEvent *timing_sorted = NULL;
    RawEventIdx *bgm_sorted = NULL;
    size_t note_pos = 0, timing_pos = 0, bgm_pos = 0;

    tls_note_time = (int *)tls_ensure_buf(tls_note_time, &tls_note_time_cap, notes->len, sizeof(int));
    note_time = tls_note_time;
    if (notes->len > 0) memset(note_time, 0, notes->len * sizeof(int));
    tls_bgm_time = (int *)tls_ensure_buf(tls_bgm_time, &tls_bgm_time_cap, bgm_raw->len, sizeof(int));
    bgm_time = tls_bgm_time;
    if (bgm_raw->len > 0) memset(bgm_time, 0, bgm_raw->len * sizeof(int));

    tp_push(timing_points, (TimingPoint){0, ctx->initial_bpm});

    double cur_time = 0.0;
    double cur_bpm = ctx->initial_bpm;
    if (notes->len > 0) {
        tls_sorted_notes = (NoteEvent *)tls_ensure_buf(tls_sorted_notes, &tls_sorted_notes_cap, notes->len, sizeof(NoteEvent));
        sorted_notes = tls_sorted_notes;
        memcpy(sorted_notes, notes->items, notes->len * sizeof(NoteEvent));
        qsort(sorted_notes, notes->len, sizeof(NoteEvent), cmp_note);
    }
    if (timing_raw->len > 0) {
        tls_timing_sorted = (RawEvent *)tls_ensure_buf(tls_timing_sorted, &tls_timing_sorted_cap, timing_raw->len, sizeof(RawEvent));
        timing_sorted = tls_timing_sorted;
        memcpy(timing_sorted, timing_raw->items, timing_raw->len * sizeof(RawEvent));
        qsort(timing_sorted, timing_raw->len, sizeof(RawEvent), cmp_raw_measure_pos);
    }
    if (bgm_raw->len > 0) {
        tls_bgm_sorted = (RawEventIdx *)tls_ensure_buf(tls_bgm_sorted, &tls_bgm_sorted_cap, bgm_raw->len, sizeof(RawEventIdx));
        bgm_sorted = tls_bgm_sorted;
        for (size_t i = 0; i < bgm_raw->len; i++) {
            bgm_sorted[i].ev = bgm_raw->items[i];
            bgm_sorted[i].orig_index = (int)i;
        }
        qsort(bgm_sorted, bgm_raw->len, sizeof(RawEventIdx), cmp_rawidx_measure_pos);
    }

    for (int m = 0; m <= last_measure; m++) {
        MeasureEventVec mev = {0};

        while (note_pos < notes->len && sorted_notes[note_pos].measure < m) note_pos++;
        for (size_t i = note_pos; i < notes->len && sorted_notes[i].measure == m; i++) {
            mev_push(&mev, (MeasureEvent){sorted_notes[i].pos, 2, 0.0, sorted_notes[i].id});
        }

        while (timing_pos < timing_raw->len && timing_sorted[timing_pos].channel / 1000 < m) timing_pos++;
        size_t t = timing_pos;
        while (t < timing_raw->len && timing_sorted[t].channel / 1000 == m) {
            int ch = timing_sorted[t].channel % 1000;
            const char *tk = timing_sorted[t].token;
            if (ch == 3) {
                char hex[3] = {tk[0], tk[1], '\0'};
                long v = strtol(hex, NULL, 16);
                if (v > 0) {
                    mev_push(&mev, (MeasureEvent){timing_sorted[t].pos, 0, (double)v, -1});
                }
            } else if (ch == 8) {
                int idx = timing_sorted[t].token_id;
                if (idx >= 0 && ctx->bpm_ext_set[idx] && ctx->bpm_ext[idx] > 0.0) {
                    mev_push(&mev, (MeasureEvent){timing_sorted[t].pos, 0, ctx->bpm_ext[idx], -1});
                }
            } else if (ch == 9) {
                int idx = timing_sorted[t].token_id;
                if (idx >= 0 && ctx->stop_ext_set[idx]) {
                    mev_push(&mev, (MeasureEvent){timing_sorted[t].pos, 1, ctx->stop_ext[idx], -1});
                }
            }
            t++;
        }
        timing_pos = t;

        while (bgm_pos < bgm_raw->len && bgm_sorted[bgm_pos].ev.channel / 1000 < m) bgm_pos++;
        size_t b = bgm_pos;
        while (b < bgm_raw->len && bgm_sorted[b].ev.channel / 1000 == m) {
            mev_push(&mev, (MeasureEvent){bgm_sorted[b].ev.pos, 3, 0.0, bgm_sorted[b].orig_index});
            b++;
        }
        bgm_pos = b;

        if (mev.len > 0) {
            qsort(mev.items, mev.len, sizeof(MeasureEvent), cmp_mev);
        }

        double measure_beats = 4.0 * ((m >= 0 && m < MAX_MEASURES) ? ctx->measure_len[m] : 1.0);
        double prev_pos = 0.0;
        size_t i = 0;
        while (i < mev.len) {
            double pos = mev.items[i].pos;
            size_t group_start = i;
            double dbeats = (pos - prev_pos) * measure_beats;
            if (dbeats > 0.0) cur_time += beats_to_ms(dbeats, cur_bpm);
            prev_pos = pos;

            while (i < mev.len && fabs(mev.items[i].pos - pos) < 1e-8) {
                if (mev.items[i].kind == 2) {
                    note_time[mev.items[i].note_index] = (int)llround(cur_time);
                } else if (mev.items[i].kind == 3) {
                    bgm_time[mev.items[i].note_index] = (int)llround(cur_time);
                }
                i++;
            }
            for (size_t j = group_start; j < i; j++) {
                if (mev.items[j].kind == 0 && mev.items[j].value > 0.0) {
                    cur_bpm = mev.items[j].value;
                    tp_push(timing_points, (TimingPoint){(int)llround(cur_time), cur_bpm});
                }
            }
            for (size_t j = group_start; j < i; j++) {
                if (mev.items[j].kind == 1 && mev.items[j].value > 0.0) {
                    double stop_beats = mev.items[j].value / 48.0;
                    cur_time += beats_to_ms(stop_beats, cur_bpm);
                }
            }
        }

        double tail = (1.0 - prev_pos) * measure_beats;
        if (tail > 0.0) cur_time += beats_to_ms(tail, cur_bpm);

        free(mev.items);
    }

    if (notes->len > 0) {
        int open_ln[18];
        int open_time[18];
        int open_wav[18];
        for (int k = 0; k < 18; k++) {
            open_ln[k] = 0;
            open_time[k] = 0;
            open_wav[k] = -1;
        }

        for (size_t i2 = 0; i2 < notes->len; i2++) {
            NoteEvent cur = sorted_notes[i2];
            int idx = cur.id;
            if (idx < 0) continue;
            int t = note_time[idx];
            if (!cur.is_ln) {
                osu_push(osu_notes, (OsuNote){cur.lane, t, t, 0, cur.wav_id});
            } else {
                if (cur.lane < 0 || cur.lane >= 18) continue;
                if (!open_ln[cur.lane]) {
                    open_ln[cur.lane] = 1;
                    open_time[cur.lane] = t;
                    open_wav[cur.lane] = cur.wav_id;
                } else {
                    int st = open_time[cur.lane];
                    int ed = t;
                    if (ed <= st) ed = st + 1;
                    osu_push(osu_notes, (OsuNote){cur.lane, st, ed, 1, open_wav[cur.lane]});
                    if (cur.wav_id >= 0) {
                        auto_push(auto_samples, (AutoSample){ed, cur.wav_id});
                    }
                    open_ln[cur.lane] = 0;
                    open_wav[cur.lane] = -1;
                }
            }
        }

        for (int k = 0; k < 18; k++) {
            if (open_ln[k]) {
                osu_push(osu_notes, (OsuNote){k, open_time[k], open_time[k] + 120, 1, open_wav[k]});
            }
        }

    }

    for (size_t i = 0; i < bgm_raw->len; i++) {
        int wav_id = bgm_raw->items[i].token_id;
        if (wav_id >= 0) {
            auto_push(auto_samples, (AutoSample){bgm_time[i], wav_id});
        }
    }
    if (auto_samples->len > 1) qsort(auto_samples->items, auto_samples->len, sizeof(AutoSample), cmp_auto);
    if (auto_samples->len > 0) {
        size_t w = 0;
        for (size_t r = 0; r < auto_samples->len; r++) {
            AutoSample a = auto_samples->items[r];
            if (w > 0) {
                AutoSample p = auto_samples->items[w - 1];
                if (p.wav_id == a.wav_id && abs(p.time_ms - a.time_ms) <= 1) continue;
            }
            auto_samples->items[w++] = a;
        }
        auto_samples->len = w;
    }
}

static void dedupe_timing_points(TimingPointVec *tp) {
    if (tp->len == 0) return;
    qsort(tp->items, tp->len, sizeof(TimingPoint), cmp_tp);
    size_t w = 0;
    for (size_t r = 0; r < tp->len; r++) {
        if (w == 0) {
            tp->items[w++] = tp->items[r];
            continue;
        }
        TimingPoint prev = tp->items[w - 1];
        TimingPoint cur = tp->items[r];
        if (prev.time_ms == cur.time_ms && fabs(prev.bpm - cur.bpm) < 1e-7) continue;
        if (fabs(prev.bpm - cur.bpm) < 1e-7) continue;
        tp->items[w++] = cur;
    }
    tp->len = w;
}

static const char *base_name_no_ext(const char *path) {
    const char *slash1 = strrchr(path, '/');
    const char *slash2 = strrchr(path, '\\');
    const char *base = path;
    if (slash1 && slash2) base = (slash1 > slash2) ? slash1 + 1 : slash2 + 1;
    else if (slash1) base = slash1 + 1;
    else if (slash2) base = slash2 + 1;
    return base;
}

static void sanitize_osu_field(char *s) {
    for (char *p = s; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '\r' || c == '\n') {
            *p = ' ';
            continue;
        }
        if (c < 32 && c != '\t') {
            *p = ' ';
        }
    }
    trim(s);
}

static int is_valid_utf8(const unsigned char *s) {
    if (!s) return 0;
    while (*s) {
        unsigned char c = *s++;
        if (c < 0x80) continue;
        if ((c & 0xE0) == 0xC0) {
            if ((s[0] & 0xC0) != 0x80) return 0;
            if ((c & 0xFE) == 0xC0) return 0;
            s += 1;
            continue;
        }
        if ((c & 0xF0) == 0xE0) {
            if ((s[0] & 0xC0) != 0x80 || (s[1] & 0xC0) != 0x80) return 0;
            if (c == 0xE0 && (s[0] & 0xE0) == 0x80) return 0;
            if (c == 0xED && (s[0] & 0xE0) == 0xA0) return 0;
            s += 2;
            continue;
        }
        if ((c & 0xF8) == 0xF0) {
            if ((s[0] & 0xC0) != 0x80 || (s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80) return 0;
            if (c == 0xF0 && (s[0] & 0xF0) == 0x80) return 0;
            if (c > 0xF4) return 0;
            if (c == 0xF4 && s[0] > 0x8F) return 0;
            s += 3;
            continue;
        }
        return 0;
    }
    return 1;
}

static int mb_to_utf8_cp(const char *src, UINT cp, char *out, size_t out_sz) {
    if (!src || !src[0] || !out || out_sz == 0) return 0;
    int wlen = MultiByteToWideChar(cp, MB_ERR_INVALID_CHARS, src, -1, NULL, 0);
    if (wlen <= 0) wlen = MultiByteToWideChar(cp, 0, src, -1, NULL, 0);
    if (wlen <= 0) return 0;
    wchar_t *wb = (wchar_t *)malloc((size_t)wlen * sizeof(wchar_t));
    if (!wb) return 0;
    int ok = 0;
    int wgot = MultiByteToWideChar(cp, 0, src, -1, wb, wlen);
    if (wgot > 0) {
        int u8len = WideCharToMultiByte(CP_UTF8, 0, wb, -1, NULL, 0, NULL, NULL);
        if (u8len > 0 && (size_t)u8len <= out_sz) {
            int ugot = WideCharToMultiByte(CP_UTF8, 0, wb, -1, out, (int)out_sz, NULL, NULL);
            if (ugot > 0) ok = 1;
        }
    }
    free(wb);
    return ok;
}

static void bms_text_to_utf8(const char *src, char *out, size_t out_sz) {
    if (!out || out_sz == 0) return;
    out[0] = '\0';
    if (!src || !src[0]) return;
    if (is_valid_utf8((const unsigned char *)src)) {
        snprintf(out, out_sz, "%s", src);
        return;
    }
    if (mb_to_utf8_cp(src, 932, out, out_sz)) return;
    if (mb_to_utf8_cp(src, CP_ACP, out, out_sz)) return;
    snprintf(out, out_sz, "%s", src);
}

static void strip_trailing_bracket_tag(const char *in, char *main_out, size_t main_sz, char *tag_out, size_t tag_sz) {
    if (main_out && main_sz) main_out[0] = '\0';
    if (tag_out && tag_sz) tag_out[0] = '\0';
    if (!in || !in[0]) return;
    if (main_out && main_sz) snprintf(main_out, main_sz, "%s", in);
    if (!main_out || !main_out[0]) return;
    trim(main_out);
    size_t n = strlen(main_out);
    if (n < 3 || main_out[n - 1] != ']') return;
    char *lb = strrchr(main_out, '[');
    if (!lb) return;
    if (lb == main_out) return;
    if (strchr(lb + 1, '[')) return;
    size_t tag_len = (size_t)(main_out + n - 1 - (lb + 1));
    if (tag_len == 0 || tag_len > 40) return;
    if (tag_out && tag_sz) {
        size_t c = tag_len < (tag_sz - 1) ? tag_len : (tag_sz - 1);
        memcpy(tag_out, lb + 1, c);
        tag_out[c] = '\0';
        trim(tag_out);
    }
    while (lb > main_out && isspace((unsigned char)lb[-1])) lb--;
    *lb = '\0';
    trim(main_out);
}

static int extract_version(const char *in, char *out, size_t out_sz) {
    const char *base = base_name_no_ext(in);
    snprintf(out, out_sz, "%s", base);
    char *dot = strrchr(out, '.');
    if (dot) *dot = '\0';
    trim(out);
    sanitize_osu_field(out);
    if (!out[0]) snprintf(out, out_sz, "Converted");
    return 1;
}

static int ci_contains(const char *s, const char *sub) {
    size_t n = strlen(sub);
    if (n == 0) return 1;
    for (const char *p = s; *p; p++) {
        size_t i = 0;
        while (i < n && p[i] && toupper((unsigned char)p[i]) == toupper((unsigned char)sub[i])) i++;
        if (i == n) return 1;
    }
    return 0;
}

static int first_number_in_text(const char *s) {
    int v = -1;
    for (const char *p = s; *p; p++) {
        if (isdigit((unsigned char)*p)) {
            v = 0;
            while (*p && isdigit((unsigned char)*p)) {
                v = v * 10 + (*p - '0');
                p++;
            }
            break;
        }
    }
    return v;
}

static void build_display_version_from_filename(const char *base_name, const char *playlevel_text, char *out, size_t out_sz) {
    const char *label = NULL;
    int def_level = 0;
    int level = -1;
    if (playlevel_text && playlevel_text[0]) {
        level = first_number_in_text(playlevel_text);
    }
    if (level <= 0) level = first_number_in_text(base_name);

    if (ci_contains(base_name, "LEGGENDARIA")) { label = "Leggendaria"; def_level = 12; }
    else if (ci_contains(base_name, "ANOTHER")) { label = "Another"; def_level = 10; }
    else if (ci_contains(base_name, "HYPER")) { label = "Hyper"; def_level = 7; }
    else if (ci_contains(base_name, "NORMAL")) { label = "Normal"; def_level = 3; }
    else if (ci_contains(base_name, "BEGINNER")) { label = "Beginner"; def_level = 1; }

    if (label) {
        if (level <= 0) level = def_level;
        snprintf(out, out_sz, "%s %d", label, level);
    } else {
        snprintf(out, out_sz, "%s", base_name);
    }
    sanitize_osu_field(out);
}

static void sanitize_filename_component(char *s) {
    for (char *p = s; *p; p++) {
        if (*p == '<' || *p == '>' || *p == ':' || *p == '"' || *p == '/' || *p == '\\' || *p == '|' || *p == '?' || *p == '*') {
            *p = '_';
        }
    }
    trim(s);
    if (!s[0]) snprintf(s, 260, "difficulty");
}

static int looks_like_dos_short_name(const char *s) {
    if (!s || !s[0]) return 0;
    const char *t = strrchr(s, '~');
    if (!t || t == s) return 0;
    if (!isdigit((unsigned char)t[1])) return 0;
    for (const char *p = t + 1; *p; p++) {
        if (!isdigit((unsigned char)*p)) return 0;
    }
    int base_len = (int)(t - s);
    if (base_len <= 0 || base_len > 8) return 0;
    for (const char *p = s; p < t; p++) {
        unsigned char c = (unsigned char)*p;
        if (!(isalnum(c) || c == '_')) return 0;
    }
    return 1;
}

static int starts_with_numeric_id(const char *s) {
    if (!s || !isdigit((unsigned char)s[0])) return 0;
    int n = 0;
    while (s[n] && isdigit((unsigned char)s[n])) n++;
    return n >= 4;
}


static uint32_t crc32_update(uint32_t crc, const unsigned char *buf, size_t len) {
    static uint32_t table[256];
    static int init = 0;
    if (!init) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            table[i] = c;
        }
        init = 1;
    }
    crc = ~crc;
    for (size_t i = 0; i < len; i++) crc = table[(crc ^ buf[i]) & 0xFFu] ^ (crc >> 8);
    return ~crc;
}

static void wr16(FILE *f, uint16_t v) {
    unsigned char b[2];
    b[0] = (unsigned char)(v & 255);
    b[1] = (unsigned char)((v >> 8) & 255);
    fwrite(b, 1, 2, f);
}

static void wr32(FILE *f, uint32_t v) {
    unsigned char b[4];
    b[0] = (unsigned char)(v & 255);
    b[1] = (unsigned char)((v >> 8) & 255);
    b[2] = (unsigned char)((v >> 16) & 255);
    b[3] = (unsigned char)((v >> 24) & 255);
    fwrite(b, 1, 4, f);
}

static int create_zip_store(const char *zip_path, const PackVec *items) {
    typedef struct {
        uint32_t crc;
        uint32_t size;
        uint32_t local_off;
        uint16_t name_len;
    } ZMeta;

    FILE *zf = fopen(zip_path, "wb");
    if (!zf) return 0;

    ZMeta *meta = (ZMeta *)xrealloc(NULL, (items->len ? items->len : 1) * sizeof(ZMeta));
    unsigned char buf[1 << 20];
    int ok = 1;

    for (size_t i = 0; i < items->len; i++) {
        FILE *in = fopen(items->items[i].src_path, "rb");
        if (!in) {
            ok = 0;
            break;
        }
        const char *name = items->items[i].arc_name;
        uint16_t nlen = (uint16_t)strlen(name);
        meta[i].name_len = nlen;
        meta[i].local_off = (uint32_t)ftell(zf);

        wr32(zf, 0x04034B50u);
        wr16(zf, 20);
        wr16(zf, 0x0008);
        wr16(zf, 0);
        wr16(zf, 0);
        wr16(zf, 0);
        wr32(zf, 0);
        wr32(zf, 0);
        wr32(zf, 0);
        wr16(zf, nlen);
        wr16(zf, 0);
        fwrite(name, 1, nlen, zf);

        {
            uint32_t crc = 0;
            uint32_t sz = 0;
            size_t rn;
            while ((rn = fread(buf, 1, sizeof(buf), in)) > 0) {
                crc = crc32_update(crc, buf, rn);
                sz += (uint32_t)rn;
                fwrite(buf, 1, rn, zf);
            }
            if (ferror(in)) {
                fclose(in);
                ok = 0;
                break;
            }
            meta[i].crc = crc;
            meta[i].size = sz;
        }
        fclose(in);

        wr32(zf, 0x08074B50u);
        wr32(zf, meta[i].crc);
        wr32(zf, meta[i].size);
        wr32(zf, meta[i].size);
    }

    uint32_t cd_off = (uint32_t)ftell(zf);
    if (ok) {
        for (size_t i = 0; i < items->len; i++) {
            const char *name = items->items[i].arc_name;
            wr32(zf, 0x02014B50u);
            wr16(zf, 20);
            wr16(zf, 20);
            wr16(zf, 0);
            wr16(zf, 0);
            wr16(zf, 0);
            wr16(zf, 0);
            wr32(zf, meta[i].crc);
            wr32(zf, meta[i].size);
            wr32(zf, meta[i].size);
            wr16(zf, meta[i].name_len);
            wr16(zf, 0);
            wr16(zf, 0);
            wr16(zf, 0);
            wr16(zf, 0);
            wr32(zf, 0);
            wr32(zf, meta[i].local_off);
            fwrite(name, 1, meta[i].name_len, zf);
        }
        uint32_t cd_sz = (uint32_t)ftell(zf) - cd_off;
        wr32(zf, 0x06054B50u);
        wr16(zf, 0);
        wr16(zf, 0);
        wr16(zf, (uint16_t)items->len);
        wr16(zf, (uint16_t)items->len);
        wr32(zf, cd_sz);
        wr32(zf, cd_off);
        wr16(zf, 0);
    }

    free(meta);
    fclose(zf);
    if (!ok) DeleteFileA(zip_path);
    return ok;
}

static int write_osu(const char *out_path, const char *input_path, const BmsContext *ctx, const OsuNoteVec *notes, const TimingPointVec *tp, const AutoSampleVec *auto_samples, int keys, const char *video_name, const char *display_version, double od_value, const char *bg_name) {
    FILE *fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "Cannot write %s: %s\n", out_path, strerror(errno));
        return 0;
    }

    char version[260];
    char title_raw[256];
    char artist_raw[256];
    char title_main_raw[256];
    char title_tag_raw[64];
    char title_buf[512];
    char artist_buf[512];
    char tags_buf[512];
    char version_out[260];
    extract_version(input_path, version, sizeof(version));

    const char *title = ctx->title[0] ? ctx->title : version;
    const char *artist = ctx->artist[0] ? ctx->artist : "Unknown Artist";
    copy_field(title_raw, sizeof(title_raw), title);
    copy_field(artist_raw, sizeof(artist_raw), artist);
    strip_trailing_bracket_tag(title_raw, title_main_raw, sizeof(title_main_raw), title_tag_raw, sizeof(title_tag_raw));
    if (!title_main_raw[0]) snprintf(title_main_raw, sizeof(title_main_raw), "%s", title_raw);
    bms_text_to_utf8(title_main_raw, title_buf, sizeof(title_buf));
    bms_text_to_utf8(artist_raw, artist_buf, sizeof(artist_buf));
    bms_text_to_utf8(ctx->genre, tags_buf, sizeof(tags_buf));
    sanitize_osu_field(title_buf);
    sanitize_osu_field(artist_buf);
    sanitize_osu_field(tags_buf);
    if (display_version && display_version[0]) snprintf(version_out, sizeof(version_out), "%s", display_version);
    else snprintf(version_out, sizeof(version_out), "%s", version);
    sanitize_osu_field(version_out);
    if (title_tag_raw[0]) {
        snprintf(version_out, sizeof(version_out), "%s", title_tag_raw);
    }

    fprintf(fp, "osu file format v14\r\n\r\n");
    fprintf(fp, "[General]\r\n");
    fprintf(fp, "Mode:3\r\n");
    fprintf(fp, "SampleSet:Soft\r\n");
    fprintf(fp, "Countdown:0\r\n");
    if (keys == 8) fprintf(fp, "SpecialStyle:1\r\n");
    fprintf(fp, "\r\n");

    fprintf(fp, "[Editor]\r\n");
    fprintf(fp, "DistanceSpacing:1\r\n");
    fprintf(fp, "BeatDivisor:1\r\n");
    fprintf(fp, "GridSize:1\r\n");
    fprintf(fp, "TimelineZoom:1\r\n\r\n");

    fprintf(fp, "[Metadata]\r\n");
    fprintf(fp, "Title:%s\r\n", title_buf);
    fprintf(fp, "TitleUnicode:%s\r\n", title_buf);
    fprintf(fp, "Artist:%s\r\n", artist_buf);
    fprintf(fp, "ArtistUnicode:%s\r\n", artist_buf);
    fprintf(fp, "Creator:bme_to_osu\r\n");
    fprintf(fp, "Version:%s\r\n", version_out);
    fprintf(fp, "Source:\r\n");
    if (tags_buf[0]) fprintf(fp, "Tags:%s\r\n", tags_buf);
    else fprintf(fp, "Tags:\r\n");
    fprintf(fp, "BeatmapID:0\r\n");
    fprintf(fp, "BeatmapSetID:0\r\n\r\n");

    fprintf(fp, "[Difficulty]\r\n");
    fprintf(fp, "HPDrainRate:7.5\r\n");
    fprintf(fp, "CircleSize:%d\r\n", keys);
    fprintf(fp, "OverallDifficulty:%.2f\r\n", od_value);
    fprintf(fp, "ApproachRate:0\r\n");
    fprintf(fp, "SliderMultiplier:1\r\n");
    fprintf(fp, "SliderTickRate:1\r\n\r\n");

    fprintf(fp, "[Events]\r\n");
    if (bg_name && bg_name[0]) {
        fprintf(fp, "0,0,\"%s\",0,0\r\n", bg_name);
    } else {
        char dir[4096], bg[4096];
        path_dirname(input_path, dir, sizeof(dir));
        join_path(bg, sizeof(bg), dir, "1.png");
        if (file_exists(bg)) {
            fprintf(fp, "0,0,\"1.png\",0,0\r\n");
        }
    }
    if (video_name && video_name[0]) {
        fprintf(fp, "Video,0,\"%s\"\r\n", video_name);
    }
    for (size_t i = 0; i < auto_samples->len; i++) {
        int wid = auto_samples->items[i].wav_id;
        if (wid < 0 || wid >= MAX_ID62) continue;
        if (!ctx->wav_defs[wid]) continue;
        fprintf(fp, "Sample,%d,0,\"%s\",100\r\n", auto_samples->items[i].time_ms, path_basename(ctx->wav_defs[wid]));
    }
    fprintf(fp, "\r\n");

    fprintf(fp, "[TimingPoints]\r\n");
    for (size_t i = 0; i < tp->len; i++) {
        double beat_len = 60000.0 / tp->items[i].bpm;
        fprintf(fp, "%d,%.15g,4,2,0,0,1,0\r\n", tp->items[i].time_ms, beat_len);
    }
    fprintf(fp, "\r\n");

    fprintf(fp, "[HitObjects]\r\n");
    for (size_t i = 0; i < notes->len; i++) {
        const OsuNote *n = &notes->items[i];
        int x = (int)floor((512.0 / keys) * n->lane + (256.0 / keys));
        const char *sample_name = "";
        if (n->wav_id >= 0 && n->wav_id < MAX_ID62 && ctx->wav_defs[n->wav_id]) {
            sample_name = path_basename(ctx->wav_defs[n->wav_id]);
        }
        if (!n->is_ln) {
            if (sample_name[0]) {
                fprintf(fp, "%d,192,%d,1,0,0:0:0:100:%s\r\n", x, n->time_ms, sample_name);
            } else {
                fprintf(fp, "%d,192,%d,1,0,0:0:0:0:\r\n", x, n->time_ms);
            }
        } else {
            if (sample_name[0]) {
                fprintf(fp, "%d,192,%d,128,0,%d:0:0:0:100:%s\r\n", x, n->time_ms, n->end_ms, sample_name);
            } else {
                fprintf(fp, "%d,192,%d,128,0,%d:0:0:0:0:\r\n", x, n->time_ms, n->end_ms);
            }
        }
    }

    fclose(fp);
    return 1;
}

static void free_context(BmsContext *ctx) {
    for (int i = 0; i < MAX_ID62; i++) {
        free(ctx->wav_defs[i]);
        ctx->wav_defs[i] = NULL;
    }
}

static int cmp_str_icase(const void *a, const void *b) {
    const char *x = *(const char *const *)a;
    const char *y = *(const char *const *)b;
    return _stricmp(x, y);
}

static int is_chart_filename(const char *name) {
    if (strstr(name, ".bme") || strstr(name, ".BME")) return 1;
    if (strstr(name, ".bms") || strstr(name, ".BMS")) return 1;
    return 0;
}

static int collect_chart_files_in_dir(const char *dir, StringVec *out) {
    size_t before = out->len;
    char pat[4096];
    snprintf(pat, sizeof(pat), "%s\\*", dir);
    WIN32_FIND_DATAA fd;
    HANDLE h = FindFirstFileA(pat, &fd);
    if (h == INVALID_HANDLE_VALUE) return 0;
    do {
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) continue;
        if (!is_chart_filename(fd.cFileName)) continue;
        char full[4096];
        join_path(full, sizeof(full), dir, fd.cFileName);
        if (!is_sp_chart_file(full)) continue;
        str_push(out, full);
    } while (FindNextFileA(h, &fd));
    FindClose(h);
    return out->len > before;
}

static int get_exe_dir(char *out, size_t out_sz) {
    DWORD n = GetModuleFileNameA(NULL, out, (DWORD)out_sz);
    if (n == 0 || n >= out_sz) return 0;
    char *s1 = strrchr(out, '/');
    char *s2 = strrchr(out, '\\');
    char *s = NULL;
    if (s1 && s2) s = (s1 > s2) ? s1 : s2;
    else if (s1) s = s1;
    else if (s2) s = s2;
    if (!s) return 0;
    *s = '\0';
    return 1;
}

static int strvec_contains_icase(const StringVec *v, const char *s) {
    for (size_t i = 0; i < v->len; i++) {
        if (_stricmp(v->items[i], s) == 0) return 1;
    }
    return 0;
}

static void collect_map_dirs(const char *target, StringVec *out_dirs) {
    if (!dir_exists(target)) return;

    if (!strvec_contains_icase(out_dirs, target)) str_push(out_dirs, target);

    wchar_t target_w[4096];
    if (!ansi_to_wide(target, target_w, sizeof(target_w) / sizeof(target_w[0]))) return;
    wchar_t pat_w[4096];
    _snwprintf(pat_w, (sizeof(pat_w) / sizeof(pat_w[0])) - 1, L"%ls\\*", target_w);
    pat_w[(sizeof(pat_w) / sizeof(pat_w[0])) - 1] = L'\0';
    WIN32_FIND_DATAW fdw;
    HANDLE h = FindFirstFileW(pat_w, &fdw);
    if (h == INVALID_HANDLE_VALUE) return;
    do {
        if (!(fdw.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) continue;
        if (wcscmp(fdw.cFileName, L".") == 0 || wcscmp(fdw.cFileName, L"..") == 0) continue;
        wchar_t sub_w[4096];
        char sub_a[4096];
        join_path_w(sub_w, sizeof(sub_w) / sizeof(sub_w[0]), target_w, fdw.cFileName);
        if (!wide_dir_to_ansi_path(sub_w, sub_a, sizeof(sub_a))) continue;
        if (!strvec_contains_icase(out_dirs, sub_a)) str_push(out_dirs, sub_a);
    } while (FindNextFileW(h, &fdw));
    FindClose(h);
}

static void collect_pack_audio_recursive(const char *dir, PackVec *pack) {
    char pat[4096];
    snprintf(pat, sizeof(pat), "%s\\*", dir);
    WIN32_FIND_DATAA fd;
    HANDLE h = FindFirstFileA(pat, &fd);
    if (h == INVALID_HANDLE_VALUE) return;
    do {
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            if (strcmp(fd.cFileName, ".") == 0 || strcmp(fd.cFileName, "..") == 0) continue;
            char subdir[4096];
            join_path(subdir, sizeof(subdir), dir, fd.cFileName);
            collect_pack_audio_recursive(subdir, pack);
            continue;
        }
        if (_stricmp(fd.cFileName, "preview_auto_generator.wav") == 0) continue;
        if (!has_audio_ext(fd.cFileName)) continue;
        {
            char src[4096];
            join_path(src, sizeof(src), dir, fd.cFileName);
            pack_push_unique(pack, src, fd.cFileName);
        }
    } while (FindNextFileA(h, &fd));
    FindClose(h);
}

static void collect_bg_images(const char *bg_dir, StringVec *bg_paths, StringVec *bg_names) {
    char pat[4096];
    snprintf(pat, sizeof(pat), "%s\\*", bg_dir);
    WIN32_FIND_DATAA fd;
    HANDLE h = FindFirstFileA(pat, &fd);
    if (h == INVALID_HANDLE_VALUE) return;
    do {
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) continue;
        if (!has_image_ext(fd.cFileName)) continue;
        {
            char full[4096];
            join_path(full, sizeof(full), bg_dir, fd.cFileName);
            str_push(bg_paths, full);
            str_push(bg_names, fd.cFileName);
        }
    } while (FindNextFileA(h, &fd));
    FindClose(h);
}

static void collect_pack_assets(const char *map_dir, PackVec *pack, int allow_map_fallback_bg) {
    collect_pack_audio_recursive(map_dir, pack);

    if (allow_map_fallback_bg) {
        char p2[4096];
        join_path(p2, sizeof(p2), map_dir, "1.png");
        if (file_exists(p2)) pack_push_unique(pack, p2, "1.png");
    }
}

static int process_single_chart(const char *input, const char *tmp_dir, int opt_add7k, int opt_only7k, int opt_addvideo, const char *picked_video, const StringVec *bg_paths, const StringVec *bg_names, PackVec *out_pack) {
    BmsContext ctx;
    RawEventVec raw = {0};
    init_context(&ctx);
    if (!parse_bms_file(input, &ctx, &raw)) {
        free_context(&ctx);
        free(raw.items);
        return 0;
    }

    char base_name[260];
    char display_version[260];
    char selected_bg_src[4096] = {0};
    char selected_bg_name[512] = {0};
    int has_custom_bg = 0;
    extract_version(input, base_name, sizeof(base_name));
    build_display_version_from_filename(base_name, ctx.difficulty, display_version, sizeof(display_version));
    if (bg_paths && bg_names && bg_paths->len > 0 && bg_paths->len == bg_names->len) {
        uint32_t h = hash_path_fnv1a(input);
        uint32_t seq = (uint32_t)InterlockedIncrement(&g_bg_pick_seq);
        size_t idx = (size_t)((h ^ (seq * 2654435761u)) % (uint32_t)bg_paths->len);
        snprintf(selected_bg_src, sizeof(selected_bg_src), "%s", bg_paths->items[idx]);
        snprintf(selected_bg_name, sizeof(selected_bg_name), "%s", bg_names->items[idx]);
        pack_push_unique(out_pack, selected_bg_src, selected_bg_name);
        has_custom_bg = 1;
    }

    int ok = 1;
    for (int pass = 0; pass < 2; pass++) {
        int do_pass = 0;
        int cut_scratch = 0;
        int keys = 8;
        if (opt_only7k) {
            if (pass == 0) { do_pass = 1; cut_scratch = 1; keys = 7; }
        } else if (opt_add7k) {
            if (pass == 0) { do_pass = 1; cut_scratch = 0; keys = 8; }
            if (pass == 1) { do_pass = 1; cut_scratch = 1; keys = 7; }
        } else {
            if (pass == 0) { do_pass = 1; cut_scratch = 0; keys = 8; }
        }
        if (!do_pass) continue;

        NoteEventVec notes = {0};
        RawEventVec timing = {0};
        RawEventVec bgm = {0};
        int max_lane = -1;
        int max_measure_notes = 0;
        split_raw_events(&raw, &notes, &timing, &bgm, &max_lane, &max_measure_notes, cut_scratch);

        int last_measure = ctx.max_measure_seen;
        if (max_measure_notes > last_measure) last_measure = max_measure_notes;
        OsuNoteVec osu = {0};
        TimingPointVec tp = {0};
        AutoSampleVec auto_samples = {0};
        char osu_name[640], osu_path[4096];

        generate_timing_and_notes(&ctx, &notes, &timing, &bgm, last_measure, &osu, &tp, &auto_samples);
        dedupe_timing_points(&tp);
        if (osu.len > 0) qsort(osu.items, osu.len, sizeof(OsuNote), cmp_osu);
        double od_value = od_from_bms_rank_fixed(ctx.rank_bms);

        if (keys == 8) {
            snprintf(osu_name, sizeof(osu_name), "%s.osu", base_name);
        } else {
            if (opt_add7k) snprintf(osu_name, sizeof(osu_name), "%s_7k.osu", base_name);
            else snprintf(osu_name, sizeof(osu_name), "%s.osu", base_name);
        }
        sanitize_filename_component(osu_name);
        join_path(osu_path, sizeof(osu_path), tmp_dir, osu_name);
        if (write_osu(osu_path, input, &ctx, &osu, &tp, &auto_samples, keys, opt_addvideo ? picked_video : "", display_version, od_value, has_custom_bg ? selected_bg_name : "")) {
            pack_push_unique(out_pack, osu_path, osu_name);
        } else {
            ok = 0;
        }

        free(osu.items);
        free(auto_samples.items);
        free(tp.items);
        free(notes.items);
        free(timing.items);
        free(bgm.items);
    }

    free_context(&ctx);
    free(raw.items);
    return ok;
}

typedef struct {
    const StringVec *charts;
    const StringVec *bg_paths;
    const StringVec *bg_names;
    const char *tmp_dir;
    int opt_add7k;
    int opt_only7k;
    int opt_addvideo;
    const char *picked_video;
    volatile LONG next_idx;
    volatile LONG any_fail;
    PackVec *results;
} ChartWorkerCtx;

static unsigned __stdcall chart_worker_proc(void *arg) {
    ChartWorkerCtx *w = (ChartWorkerCtx *)arg;
    for (;;) {
        LONG idx = InterlockedIncrement(&w->next_idx);
        if (idx < 0 || (size_t)idx >= w->charts->len) break;
        if (!process_single_chart(
                w->charts->items[idx],
                w->tmp_dir,
                w->opt_add7k,
                w->opt_only7k,
                w->opt_addvideo,
                w->picked_video,
                w->bg_paths,
                w->bg_names,
                &w->results[idx])) {
            InterlockedExchange(&w->any_fail, 1);
        }
    }
    return 0;
}

typedef struct {
    PackVec pack;
    char osz_path[4096];
    char tmp_dir[4096];
    wchar_t desired_osz_path_w[4096];
    int ok;
} ZipTask;

static void log_created_path(const char *ansi_path, const wchar_t *wide_path) {
    HANDLE h = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD mode = 0;
    if (h != INVALID_HANDLE_VALUE && h != NULL && GetConsoleMode(h, &mode)) {
        DWORD wr = 0;
        static const wchar_t prefix[] = L"Created: ";
        static const wchar_t nl[] = L"\r\n";
        WriteConsoleW(h, prefix, (DWORD)(sizeof(prefix) / sizeof(prefix[0]) - 1), &wr, NULL);
        if (wide_path && wide_path[0]) WriteConsoleW(h, wide_path, (DWORD)wcslen(wide_path), &wr, NULL);
        else if (ansi_path && ansi_path[0]) {
            wchar_t wtmp[4096];
            if (ansi_to_wide(ansi_path, wtmp, sizeof(wtmp) / sizeof(wtmp[0]))) {
                WriteConsoleW(h, wtmp, (DWORD)wcslen(wtmp), &wr, NULL);
            } else {
                WriteConsoleW(h, L"(path)", 6, &wr, NULL);
            }
        }
        WriteConsoleW(h, nl, 2, &wr, NULL);
        return;
    }
    printf("Created: %s\n", ansi_path ? ansi_path : "");
}

static unsigned __stdcall zip_worker_proc(void *arg) {
    ZipTask *z = (ZipTask *)arg;
    z->ok = create_zip_store(z->osz_path, &z->pack);
    return 0;
}

static void cleanup_tmp_from_pack(const char *tmp_dir, PackVec *pack) {
    size_t n = strlen(tmp_dir);
    for (size_t i = 0; i < pack->len; i++) {
        if (_strnicmp(pack->items[i].src_path, tmp_dir, n) == 0) {
            DeleteFileA(pack->items[i].src_path);
        }
    }
    RemoveDirectoryA(tmp_dir);
}

static void finish_zip_task(ZipTask *z, int *overall_ok) {
    if (!z) return;
    if (!z->ok) {
        fprintf(stderr, "Failed to create archive: %s\n", z->osz_path);
        if (overall_ok) *overall_ok = 0;
    } else {
        const wchar_t *printed_w = NULL;
        if (z->desired_osz_path_w[0]) {
            wchar_t src_w[4096];
            if (ansi_to_wide(z->osz_path, src_w, sizeof(src_w) / sizeof(src_w[0])) &&
                _wcsicmp(src_w, z->desired_osz_path_w) != 0) {
                MoveFileExW(src_w, z->desired_osz_path_w, MOVEFILE_REPLACE_EXISTING);
            }
            printed_w = z->desired_osz_path_w;
        }
        log_created_path(z->osz_path, printed_w);
    }
    cleanup_tmp_from_pack(z->tmp_dir, &z->pack);
    pack_free(&z->pack);
    z->tmp_dir[0] = '\0';
    z->osz_path[0] = '\0';
    z->desired_osz_path_w[0] = L'\0';
}

int main(int argc, char **argv) {
    LARGE_INTEGER perf_freq, perf_start, perf_end;
    QueryPerformanceFrequency(&perf_freq);
    QueryPerformanceCounter(&perf_start);
    int wargc = 0;
    LPWSTR *wargv = CommandLineToArgvW(GetCommandLineW(), &wargc);
    if ((wargv && wargc < 2) || (!wargv && argc < 2)) {
        fprintf(stderr, "Usage: %s <map_folder OR root_folder_with_map_subfolders> [-add7k|-only7k] [-addvideo]\n", argv[0]);
        if (wargv) LocalFree(wargv);
        return 1;
    }
    const char *target_arg = argv[1];
    int opt_add7k = 0;
    int opt_only7k = 0;
    int opt_addvideo = 0;
    if (wargv) {
        for (int i = 2; i < wargc; i++) {
            if (_wcsicmp(wargv[i], L"-add7k") == 0) opt_add7k = 1;
            else if (_wcsicmp(wargv[i], L"-only7k") == 0) opt_only7k = 1;
            else if (_wcsicmp(wargv[i], L"-addvideo") == 0) opt_addvideo = 1;
            else {
                char bad[1024];
                if (!wide_to_ansi(wargv[i], bad, sizeof(bad))) snprintf(bad, sizeof(bad), "<unicode-flag>");
                fprintf(stderr, "Unknown flag: %s\n", bad);
                LocalFree(wargv);
                return 1;
            }
        }
    } else {
        for (int i = 2; i < argc; i++) {
            if (_stricmp(argv[i], "-add7k") == 0) opt_add7k = 1;
            else if (_stricmp(argv[i], "-only7k") == 0) opt_only7k = 1;
            else if (_stricmp(argv[i], "-addvideo") == 0) opt_addvideo = 1;
            else {
                fprintf(stderr, "Unknown flag: %s\n", argv[i]);
                return 1;
            }
        }
    }
    if (opt_add7k && opt_only7k) {
        fprintf(stderr, "Flags -add7k and -only7k cannot be used together.\n");
        if (wargv) LocalFree(wargv);
        return 1;
    }

    char exe_dir[4096], out_dir[4096];
    wchar_t out_dir_w[4096];
    if (!get_exe_dir(exe_dir, sizeof(exe_dir))) {
        fprintf(stderr, "Cannot resolve exe directory.\n");
        return 1;
    }
    char target[4096];
    int resolved = 0;
    if (wargv && wargc > 1) {
        wchar_t exe_dir_w[4096];
        int wn = MultiByteToWideChar(CP_ACP, 0, exe_dir, -1, exe_dir_w, (int)(sizeof(exe_dir_w) / sizeof(exe_dir_w[0])));
        if (wn > 0) resolved = resolve_input_dir_w(wargv[1], exe_dir_w, target, sizeof(target));
    }
    if (wargv) LocalFree(wargv);
    if (!resolved) resolved = resolve_input_dir(target_arg, exe_dir, target, sizeof(target));
    if (!resolved) {
        fprintf(stderr, "Input folder not found: %s\n", target_arg);
        return 1;
    }
    join_path(out_dir, sizeof(out_dir), exe_dir, "output");
    CreateDirectoryA(out_dir, NULL);
    if (!ansi_to_wide(out_dir, out_dir_w, sizeof(out_dir_w) / sizeof(out_dir_w[0]))) {
        out_dir_w[0] = L'\0';
    }

    StringVec map_dirs = {0};
    StringVec bg_paths = {0};
    StringVec bg_names = {0};
    {
        char bg_dir[4096];
        join_path(bg_dir, sizeof(bg_dir), exe_dir, "bg");
        collect_bg_images(bg_dir, &bg_paths, &bg_names);
    }
    int has_external_bg = (bg_paths.len > 0);
    collect_map_dirs(target, &map_dirs);
    if (map_dirs.len == 0) {
        fprintf(stderr, "No SP chart folders with .bme/.bms found in: %s\n", target);
        return 1;
    }

    int overall_ok = 1;
    HANDLE zip_th = NULL;
    ZipTask zip_pending;
    memset(&zip_pending, 0, sizeof(zip_pending));
    int zip_pending_active = 0;
    for (size_t mdi = 0; mdi < map_dirs.len; mdi++) {
        const char *map_dir = map_dirs.items[mdi];
        StringVec charts = {0};
        collect_chart_files_in_dir(map_dir, &charts);
        if (charts.len == 0) {
            str_free(&charts);
            continue;
        }
        qsort(charts.items, charts.len, sizeof(char *), cmp_str_icase);

        char map_name[260];
        copy_field(map_name, sizeof(map_name), path_basename(map_dir));
        sanitize_filename_component(map_name);
        if ((map_name[0] == '\0' || (looks_like_dos_short_name(map_name) && !starts_with_numeric_id(map_name))) && charts.len > 0) {
            copy_field(map_name, sizeof(map_name), path_basename(charts.items[0]));
            char *dot = strrchr(map_name, '.');
            if (dot) *dot = '\0';
            sanitize_filename_component(map_name);
        }
        if (map_name[0] == '\0') copy_field(map_name, sizeof(map_name), "map");

        char tmp_dir[4096];
        {
            char tmp_name[320];
            snprintf(tmp_name, sizeof(tmp_name), "_tmp_%s_%lu", map_name, (unsigned long)GetCurrentProcessId());
            join_path(tmp_dir, sizeof(tmp_dir), out_dir, tmp_name);
        }
        CreateDirectoryA(tmp_dir, NULL);

        PackVec pack = {0};
        collect_pack_assets(map_dir, &pack, has_external_bg ? 0 : 1);
        char picked_video[512] = {0};
        if (opt_addvideo && find_video_over_1min(map_dir, picked_video, sizeof(picked_video))) {
            char vsrc[4096];
            join_path(vsrc, sizeof(vsrc), map_dir, picked_video);
            pack_push_unique(&pack, vsrc, picked_video);
        }

        {
            PackVec *results = (PackVec *)calloc(charts.len ? charts.len : 1, sizeof(PackVec));
            int cpu = (int)GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            if (cpu <= 0) cpu = 4;
            int threads_n = cpu;
            if (threads_n > 64) threads_n = 64;
            if ((size_t)threads_n > charts.len) threads_n = (int)charts.len;
            if (threads_n < 1) threads_n = 1;

            ChartWorkerCtx w;
            w.charts = &charts;
            w.bg_paths = &bg_paths;
            w.bg_names = &bg_names;
            w.tmp_dir = tmp_dir;
            w.opt_add7k = opt_add7k;
            w.opt_only7k = opt_only7k;
            w.opt_addvideo = opt_addvideo;
            w.picked_video = picked_video;
            w.next_idx = -1;
            w.any_fail = 0;
            w.results = results;

            HANDLE th[64];
            unsigned tid = 0;
            int created = 0;
            for (int t = 0; t < threads_n; t++) {
                th[t] = (HANDLE)_beginthreadex(NULL, 0, chart_worker_proc, &w, 0, &tid);
                if (!th[t]) break;
                created++;
            }
            if (created == 0) {
                for (size_t i = 0; i < charts.len; i++) {
                    if (!process_single_chart(charts.items[i], tmp_dir, opt_add7k, opt_only7k, opt_addvideo, picked_video, &bg_paths, &bg_names, &results[i])) {
                        overall_ok = 0;
                    }
                }
            } else {
                WaitForMultipleObjects((DWORD)created, th, TRUE, INFINITE);
                for (int t = 0; t < created; t++) CloseHandle(th[t]);
                if (w.any_fail) overall_ok = 0;
            }

            for (size_t i = 0; i < charts.len; i++) {
                for (size_t j = 0; j < results[i].len; j++) {
                    pack_push_unique(&pack, results[i].items[j].src_path, results[i].items[j].arc_name);
                }
                pack_free(&results[i]);
            }
            free(results);
        }

        char osz_name[320], osz_path[4096];
        snprintf(osz_name, sizeof(osz_name), "%s.osz", map_name);
        join_path(osz_path, sizeof(osz_path), out_dir, osz_name);
        wchar_t desired_osz_w[4096];
        desired_osz_w[0] = L'\0';
        if (out_dir_w[0]) {
            wchar_t map_w[4096], map_long_w[4096], map_base_w[4096], final_name_w[512];
            if (ansi_to_wide(map_dir, map_w, sizeof(map_w) / sizeof(map_w[0]))) {
                DWORD gl = GetLongPathNameW(map_w, map_long_w, (DWORD)(sizeof(map_long_w) / sizeof(map_long_w[0])));
                const wchar_t *src = (gl > 0 && gl < (DWORD)(sizeof(map_long_w) / sizeof(map_long_w[0]))) ? map_long_w : map_w;
                const wchar_t *b1 = wcsrchr(src, L'\\');
                const wchar_t *b2 = wcsrchr(src, L'/');
                const wchar_t *base = src;
                if (b1 && b2) base = (b1 > b2) ? (b1 + 1) : (b2 + 1);
                else if (b1) base = b1 + 1;
                else if (b2) base = b2 + 1;
                wcsncpy(map_base_w, base, (sizeof(map_base_w) / sizeof(map_base_w[0])) - 1);
                map_base_w[(sizeof(map_base_w) / sizeof(map_base_w[0])) - 1] = L'\0';
                _snwprintf(final_name_w, (sizeof(final_name_w) / sizeof(final_name_w[0])) - 1, L"%ls.osz", map_base_w);
                final_name_w[(sizeof(final_name_w) / sizeof(final_name_w[0])) - 1] = L'\0';
                join_path_w(desired_osz_w, sizeof(desired_osz_w) / sizeof(desired_osz_w[0]), out_dir_w, final_name_w);
            }
        }
        if (zip_pending_active) {
            WaitForSingleObject(zip_th, INFINITE);
            CloseHandle(zip_th);
            zip_th = NULL;
            finish_zip_task(&zip_pending, &overall_ok);
            zip_pending_active = 0;
        }

        zip_pending.pack = pack;
        pack.items = NULL;
        pack.len = pack.cap = 0;
        pack.arcs.keys = NULL;
        pack.arcs.cap = pack.arcs.len = 0;
        snprintf(zip_pending.tmp_dir, sizeof(zip_pending.tmp_dir), "%s", tmp_dir);
        snprintf(zip_pending.osz_path, sizeof(zip_pending.osz_path), "%s", osz_path);
        wcsncpy(zip_pending.desired_osz_path_w, desired_osz_w, (sizeof(zip_pending.desired_osz_path_w) / sizeof(zip_pending.desired_osz_path_w[0])) - 1);
        zip_pending.desired_osz_path_w[(sizeof(zip_pending.desired_osz_path_w) / sizeof(zip_pending.desired_osz_path_w[0])) - 1] = L'\0';
        zip_pending.ok = 0;

        {
            unsigned ztid = 0;
            zip_th = (HANDLE)_beginthreadex(NULL, 0, zip_worker_proc, &zip_pending, 0, &ztid);
            if (!zip_th) {
                zip_pending.ok = create_zip_store(zip_pending.osz_path, &zip_pending.pack);
                finish_zip_task(&zip_pending, &overall_ok);
                zip_pending_active = 0;
            } else {
                zip_pending_active = 1;
            }
        }
        str_free(&charts);
    }

    if (zip_pending_active) {
        WaitForSingleObject(zip_th, INFINITE);
        CloseHandle(zip_th);
        zip_th = NULL;
        finish_zip_task(&zip_pending, &overall_ok);
        zip_pending_active = 0;
    }

    str_free(&bg_paths);
    str_free(&bg_names);
    str_free(&map_dirs);
    QueryPerformanceCounter(&perf_end);
    {
        long long dt = perf_end.QuadPart - perf_start.QuadPart;
        double ms = (double)dt * 1000.0 / (double)perf_freq.QuadPart;
        printf("ExecutionTimeMs: %.0f\n", ms);
    }
    return overall_ok ? 0 : 1;
}
