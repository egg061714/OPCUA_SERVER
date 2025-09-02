// Build:
// gcc -std=c99 -O2 mqtt_opcua.c open62541.c -o mqtt_opcua \
//     -DUA_ARCH_POSIX -pthread -ldl -lm -lmosquitto
#include "open62541.h"
#include <mosquitto.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <cjson/cJSON.h>

static volatile UA_Boolean running = true;
static void stopHandler(int s){ (void)s; running = false; }

static UA_Server *g_server = NULL;
static UA_NodeId  g_folderId;
static UA_NodeId  g_msgStrId, g_curQtyId, g_setQtyId, g_tempId, g_statusStrId,g_runningTimeId,g_deviceIDId,g_errocodeId;



/* --- 超輕量 JSON 取值 --- */
static UA_Boolean json_get_number(const char *json, const char *key, double *out) {
    char pat[128];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(json, pat);
    if(!p) return false;

    p = strchr(p + strlen(pat), ':');
    if(!p) return false;
    p++;
    while(*p==' '||*p=='\t') p++;

    /* case 1: "123" 這種字串數字 */
    if(*p == '\"') {
        p++;
        const char *q = strchr(p, '\"');
        if(!q) return false;
        char buf[64];
        size_t n = (size_t)(q - p);
        if(n >= sizeof(buf)) n = sizeof(buf)-1;
        memcpy(buf, p, n);
        buf[n] = '\0';

        char *ep = NULL;
        double v = strtod(buf, &ep);
        if(ep && ep != buf) { *out = v; return true; }
        return false;
    }

    /* case 2: 123 / 123.45 / -1.2e3 */
    char *ep = NULL;
    double v = strtod(p, &ep);
    if(ep && ep != p) { *out = v; return true; }

    return false;
}

static UA_Boolean json_get_string(const char *j,const char *k,char *buf,size_t n){
    char pat[128]; snprintf(pat,sizeof(pat),"\"%s\"",k);
    const char *p=strstr(j,pat); if(!p) return false;
    p=strchr(p+strlen(pat),':'); if(!p) return false; p++;
    while(*p==' '||*p=='\t') p++;
    if(*p!='\"') return false; p++; const char *q=strchr(p,'\"'); if(!q) return false;
    size_t m=(size_t)(q-p); if(m>=n) m=n-1; memcpy(buf,p,m); buf[m]='\0'; return true;
}

/* --- 寫值（觸發 DataChange 推送） --- */
static void write_string(UA_Server *s, UA_NodeId id, const char *str){
    UA_Variant v; UA_Variant_init(&v); UA_String u = UA_STRING_ALLOC((char*)str);
    UA_Variant_setScalar(&v,&u,&UA_TYPES[UA_TYPES_STRING]); UA_Server_writeValue(s,id,v); UA_String_clear(&u);
}
static void write_double(UA_Server *s, UA_NodeId id, double d){
    UA_Variant v; UA_Variant_init(&v); UA_Double x=(UA_Double)d;
    UA_Variant_setScalar(&v,&x,&UA_TYPES[UA_TYPES_DOUBLE]); UA_Server_writeValue(s,id,v);
}
#include <ctype.h>

/* --------- 寬鬆解析：支援 {key:100} 與 {"key":100} 與 "100" ---------- */
static void trim_span(const char **p, const char **q){
    while(*p < *q && isspace((unsigned char)**p)) (*p)++;
    while(*q > *p && isspace((unsigned char)*((*q)-1))) (*q)--;
}
static void slice_copy(const char *p, const char *q, char *out, size_t n){
    size_t len = (size_t)(q - p);
    if(len >= n) len = n - 1;
    memcpy(out, p, len); out[len] = '\0';
}
static void strip_quotes_inplace(char *s){
    size_t n = strlen(s);
    if(n >= 2 && ((s[0]=='"' && s[n-1]=='"') || (s[0]=='\'' && s[n-1]=='\''))){
        memmove(s, s+1, n-2); s[n-2] = '\0';
    }
}

/* 找 key 的值（key 可帶/不帶引號），回傳 value 的「原文切片」 */
static int find_value_span(const char *src, const char *key,
                           const char **valBeg, const char **valEnd){
    size_t keylen = strlen(key);
    const char *p = src;

    while((p = strstr(p, key))){
        /* 確保不是較長鍵名的一部分（前一字元是引號或非識別字元） */
        if(p > src){
            char c = *(p-1);
            if(isalnum((unsigned char)c) || c=='_') { p += keylen; continue; }
        }
        const char *kEnd = p + keylen;

        /* 允許 "key" 或 key：往左看是否有引號，不強制匹配 */
        const char *after = kEnd;
        while(*after && isspace((unsigned char)*after)) after++;
        if(*after != ':'){              /* 沒有直接冒號，可能是 "key" */
            if(*(p-1) == '"'){          /* 形式："key": */
                const char *colon = strchr(after, ':');
                if(!colon){ p += keylen; continue; }
                after = colon;
            }else{
                p += keylen; continue;
            }
        }
        /* 現在 *after == ':' */
        after++;
        while(*after && isspace((unsigned char)*after)) after++;

        const char *q = after;
        if(*q == '"' || *q == '\''){    /* 字串值：找下一個對應引號 */
            char quote = *q; q++;
            while(*q && *q != quote) q++;
            if(*q == quote) q++;        /* 包含尾引號 */
        }else{
            /* 數字或裸字串：讀到 , 或 } 為止 */
            while(*q && *q!=',' && *q!='}') q++;
        }
        *valBeg = after; *valEnd = q;
        return 1;
    }
    return 0;
}

/* 嘗試抓 number；抓不到回傳 0 */
static int get_number_relaxed(const char *src, const char *key, double *out){
    const char *b,*e;
    if(!find_value_span(src,key,&b,&e)) return 0;
    char buf[128];
    const char *sb=b, *se=e; trim_span(&sb,&se);
    slice_copy(sb,se,buf,sizeof(buf));
    strip_quotes_inplace(buf);
    char *end=NULL; double v = strtod(buf,&end);
    if(end && *end=='\0'){ *out=v; return 1; }
    return 0;
}

/* 嘗試抓 string（去掉引號） */
static int get_string_relaxed(const char *src, const char *key, char *out, size_t n){
    const char *b,*e;
    if(!find_value_span(src,key,&b,&e)) return 0;
    const char *sb=b, *se=e; trim_span(&sb,&se);
    slice_copy(sb,se,out,n);
    strip_quotes_inplace(out);
    return 1;
}

/* --- 建 UA 節點（純 C helper） --- */
static void addVarStr(UA_Server*s,const char*browse,const char*disp,UA_NodeId*out){
    UA_VariableAttributes a=UA_VariableAttributes_default; UA_String empty=UA_STRING_ALLOC("");
    UA_Variant_setScalar(&a.value,&empty,&UA_TYPES[UA_TYPES_STRING]);
    a.displayName=UA_LOCALIZEDTEXT("en-US",(char*)disp); a.dataType=UA_TYPES[UA_TYPES_STRING].typeId;
    a.accessLevel=UA_ACCESSLEVELMASK_READ|UA_ACCESSLEVELMASK_WRITE;
    UA_Server_addVariableNode(s,UA_NODEID_STRING(1,(char*)browse),g_folderId,
        UA_NODEID_NUMERIC(0,UA_NS0ID_HASCOMPONENT),UA_QUALIFIEDNAME(1,(char*)disp),
        UA_NODEID_NUMERIC(0,UA_NS0ID_BASEDATAVARIABLETYPE),a,NULL,out);
    UA_String_clear(&empty);
}
static void addVarDbl(UA_Server*s,const char*browse,const char*disp,UA_NodeId*out){
    UA_VariableAttributes a=UA_VariableAttributes_default; UA_Double z=0.0;
    UA_Variant_setScalar(&a.value,&z,&UA_TYPES[UA_TYPES_DOUBLE]);
    a.displayName=UA_LOCALIZEDTEXT("en-US",(char*)disp); a.dataType=UA_TYPES[UA_TYPES_DOUBLE].typeId;
    a.accessLevel=UA_ACCESSLEVELMASK_READ|UA_ACCESSLEVELMASK_WRITE;
    UA_Server_addVariableNode(s,UA_NODEID_STRING(1,(char*)browse),g_folderId,
        UA_NODEID_NUMERIC(0,UA_NS0ID_HASCOMPONENT),UA_QUALIFIEDNAME(1,(char*)disp),
        UA_NODEID_NUMERIC(0,UA_NS0ID_BASEDATAVARIABLETYPE),a,NULL,out);
}

static void addVariables(UA_Server*s){
    UA_ObjectAttributes o=UA_ObjectAttributes_default;
    o.displayName=UA_LOCALIZEDTEXT("en-US","MQTT");
    UA_Server_addObjectNode(s,UA_NODEID_STRING(1,"MQTT"),
        UA_NODEID_NUMERIC(0,UA_NS0ID_OBJECTSFOLDER),
        UA_NODEID_NUMERIC(0,UA_NS0ID_ORGANIZES),
        UA_QUALIFIEDNAME(1,"MQTT"),
        UA_NODEID_NUMERIC(0,UA_NS0ID_FOLDERTYPE),o,NULL,&g_folderId);

    addVarStr(s,"MQTT.Status","Status",&g_statusStrId);
    addVarDbl(s,"MQTT.CurrentQty","CurrentQty",&g_curQtyId);
    addVarDbl(s,"MQTT.SetQty","SetQty",&g_setQtyId);
    addVarDbl(s,"MQTT.Temp","Temp",&g_tempId);
    addVarDbl(s,"MQTT.runningTime","runningTime",&g_runningTimeId);
    addVarStr(s,"MQTT.deviceID","deviceID",&g_deviceIDId);
    addVarDbl(s,"MQTT.errocode","errocode",&g_errocodeId);
}

static void on_message(struct mosquitto *m, void *ud,
                       const struct mosquitto_message *msg){
    (void)m; (void)ud;
    if(!msg || !msg->payload) return;

    const char *payload = (const char*)msg->payload;
    write_string(g_server, g_msgStrId, payload);   // 原文也存著

    double v;
    char   s[128];
    char a[128];
    int hit = 0;

    if(get_number_relaxed(payload, "currentQty", &v)){ write_double(g_server, g_curQtyId, v); printf("[parse] currentQty=%.3f\n", v); hit++; }
    if(get_number_relaxed(payload, "setQty",     &v)){ write_double(g_server, g_setQtyId, v); printf("[parse] setQty=%.3f\n",     v); hit++; }
    if(get_number_relaxed(payload, "temp",       &v)){ write_double(g_server, g_tempId,   v); printf("[parse] temp=%.3f\n",       v); hit++; }
    if(get_string_relaxed(payload, "status", s, sizeof(s))){ write_string(g_server, g_statusStrId, s); printf("[parse] status=%s\n", s); hit++; }

    if(get_number_relaxed(payload, "runningTime", &v)){ write_double(g_server, g_runningTimeId, v); printf("[parse] runningTime=%.3f\n", v); hit++; }
    if(get_number_relaxed(payload, "errocode",     &v)){ write_double(g_server, g_errocodeId, v); printf("[parse] errocode=%.3f\n",     v); hit++; }
   if(get_string_relaxed(payload, "deviceID", a, sizeof(a))){ write_string(g_server, g_deviceIDId, a); printf("[parse] deviceID=%s\n", a); hit++; }
    
    printf("[MQTT] %s  %s\n", msg->topic, payload);
    if(hit==0) printf("[warn] no fields matched. Check key names & format.\n");
    fflush(stdout);
}


/* 參數：./mqtt_opcua <host> <port> <topic> [user] [pass] [cafile] */
int main(int argc, char **argv){
    const char *host  = (argc>1? argv[1] : "localhost");
    int         port  = (argc>2? atoi(argv[2]) : 1883);
    const char *topic = (argc>3? argv[3] : "esp32/data");
    const char *user  = (argc>4? argv[4] : NULL);
    const char *pass  = (argc>5? argv[5] : NULL);
    const char *caf   = (argc>6? argv[6] : NULL);  // 提供則啟用 TLS

    signal(SIGINT, stopHandler); signal(SIGTERM, stopHandler);

    /* UA Server */
    g_server = UA_Server_new();
    UA_ServerConfig *cfg = UA_Server_getConfig(g_server);
    UA_ServerConfig_setDefault(cfg);
    addVariables(g_server);
    UA_StatusCode urc = UA_Server_run_startup(g_server);
    if(urc != UA_STATUSCODE_GOOD){ fprintf(stderr,"UA startup failed\n"); return 1; }
    printf("OPC UA: opc.tcp://0.0.0.0:4840\n");

    /* MQTT */
    mosquitto_lib_init();
    struct mosquitto *mq = mosquitto_new("opcua-bridge", true, NULL);
    if(!mq){ fprintf(stderr,"mosquitto_new failed\n"); return 2; }
    mosquitto_message_callback_set(mq, on_message);
    if(user && pass) mosquitto_username_pw_set(mq, user, pass);
    if(caf){
        if(mosquitto_tls_set(mq, caf, NULL, NULL, NULL, NULL)!=MOSQ_ERR_SUCCESS){
            fprintf(stderr,"TLS set failed (CA file)\n"); return 3;
        }
    }
    if(mosquitto_connect(mq, host, port, 60)!=MOSQ_ERR_SUCCESS){
        fprintf(stderr,"MQTT connect failed (%s:%d)\n", host, port); return 4;
    }
    if(mosquitto_subscribe(mq, NULL, topic, 0)!=MOSQ_ERR_SUCCESS){
        fprintf(stderr,"MQTT subscribe failed (%s)\n", topic); return 5;
    }
    printf("MQTT: %s:%d  topic=%s  %s\n", host, port, topic, caf?"(TLS)":"");

    /* 主迴圈 */
    while(running){
        UA_Server_run_iterate(g_server, true);
        int rc = mosquitto_loop(mq, 0, 1);
        if(rc != MOSQ_ERR_SUCCESS){ mosquitto_reconnect(mq); usleep(200*1000); }
    }
    mosquitto_disconnect(mq); mosquitto_destroy(mq); mosquitto_lib_cleanup();
    UA_Server_run_shutdown(g_server); UA_Server_delete(g_server);
    return 0;
}
