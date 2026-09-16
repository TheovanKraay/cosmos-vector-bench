#!/usr/bin/env python3
# Prove the HTTPS emulator data plane accepts the benchmark doc shape via REST.
import base64, hashlib, hmac, json, urllib.parse, datetime, random, uuid, ssl, urllib.request

HOST = "https://localhost:18081"
KEY  = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
DB, COLL = "benchmark", "vectors"
ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE

def auth(verb, rtype, rid):
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    text = f"{verb.lower()}\n{rtype.lower()}\n{rid}\n{now.lower()}\n\n"
    sig = base64.b64encode(hmac.new(base64.b64decode(KEY), text.encode('utf-8'), hashlib.sha256).digest()).decode()
    return urllib.parse.quote(f"type=master&ver=1.0&sig={sig}", safe='-_.!~*\'()'), now

def req(verb, path, rtype, rid, body=None, extra=None):
    a, now = auth(verb, rtype, rid)
    h = {"authorization": a, "x-ms-date": now, "x-ms-version": "2018-12-31", "Content-Type":"application/json"}
    if extra: h.update(extra)
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(f"{HOST}/{path}", data=data, headers=h, method=verb)
    try:
        with urllib.request.urlopen(r, context=ctx, timeout=15) as resp:
            return resp.status, resp.read().decode()[:200]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]

# db
print("create db:", req("POST","dbs","dbs","", {"id":DB}))
# collection with /docid pk
coll = {"id":COLL, "partitionKey":{"paths":["/docid"],"kind":"Hash"}}
print("create coll:", req("POST",f"dbs/{DB}/colls","colls",f"dbs/{DB}", coll))
# insert 5 benchmark-shaped docs
ok=0
for i in range(5):
    docid=str(uuid.uuid4())
    doc={"id":str(uuid.uuid4()),"docid":docid,"title":f"Document {i}",
         "text":"x"*200,"emb":[random.uniform(-1,1) for _ in range(16)]}
    st,_=req("POST",f"dbs/{DB}/colls/{COLL}/docs","docs",f"dbs/{DB}/colls/{COLL}",doc,
             {"x-ms-documentdb-partitionkey":json.dumps([docid])})
    if st in (200,201): ok+=1
print(f">>> inserted {ok}/5 docs")
# count via query
q={"query":"SELECT VALUE COUNT(1) FROM c"}
st,body=req("POST",f"dbs/{DB}/colls/{COLL}/docs","docs",f"dbs/{DB}/colls/{COLL}",q,
            {"x-ms-documentdb-isquery":"true","Content-Type":"application/query+json",
             "x-ms-documentdb-query-enablecrosspartition":"true"})
print(f">>> query status={st} body={body}")
print(">>> EMULATOR DATAPLANE OK" if ok==5 else ">>> DATAPLANE FAILED")
