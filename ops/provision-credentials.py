#!/usr/bin/env python3
"""Stage scoped credentials and write only SealedSecrets to the repository.
Run locally with --cert and --kubeseal. Existing cluster credentials are reused.
This does not switch broker authentication or modify PostgreSQL roles.
"""
import argparse, base64, hashlib, json, pathlib, secrets, subprocess


def remote(args, data=None):
    import shlex
    return subprocess.check_output(['ssh','the-bear','sudo -n '+shlex.join(args)],input=data)


def read_secret(namespace,name):
    raw=remote(['kubectl','get','secret',name,'-n',namespace,'--ignore-not-found','-o','json'])
    if not raw.strip():
        return {}
    obj=json.loads(raw)
    return {k:base64.b64decode(v).decode() for k,v in obj.get('data',{}).items()}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--cert',required=True)
    parser.add_argument('--kubeseal',required=True)
    args=parser.parse_args()
    def ensure(namespace,name,values,path,reuse=True):
        if reuse:
            existing=read_secret(namespace,name)
            if existing:
                values=existing
        obj={'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':namespace},'type':'Opaque','stringData':values}
        raw=json.dumps(obj).encode()
        sealed=subprocess.check_output([args.kubeseal,'--cert',args.cert,'--format','yaml'],input=raw)
        pathlib.Path(path).write_bytes(sealed)
        remote(['kubectl','apply','-f','-'],raw)
        print('Staged',namespace+'/'+name)
        return values
    mqtt=ensure('iot','pi-mix-mqtt',{'username':'pi-mix','password':secrets.token_hex(32)},'k8s/mqtt-credentials-sealed.yaml')
    ensure('database','pi-mix-mqtt',mqtt,'k8s/database/mqtt-credentials-sealed.yaml')
    ensure('iot','bridge-control',{'token':secrets.token_hex(32)},'k8s/bridge-control-sealed.yaml')
    pg=ensure('database','pi-mix-pg',{'POSTGRES_USER':'pi_mix_writer','POSTGRES_PASSWORD':secrets.token_hex(32),'POSTGRES_DB':'pi_mix'},'k8s/database/pi-mix-pg-sealed.yaml')
    cache=ensure('iot','pi-mix-redis',{'username':'pi-mix','password':secrets.token_hex(32)},'k8s/redis-credentials-sealed.yaml')
    admin=ensure('database','redis-admin',{'username':'admin','password':secrets.token_hex(32)},'infrastructure/database/redis-admin-sealed.yaml')
    # Redis is the primary, always-readable history store. The ingestor merges
    # samples via an atomic Lua script (dedup + raw/15m zsets + dirty hash),
    # trims stale buckets, backfills on cold start, and reads back aggregates
    # for the hourly roll-up. The API only reads ZSETs. The ACL must name every
    # command the script uses because Redis checks them against the caller.
    acl='user default off\nuser admin on #'+hashlib.sha256(admin['password'].encode()).hexdigest()+' ~* &* +@all\nuser pi-mix on #'+hashlib.sha256(cache['password'].encode()).hexdigest()+' ~pi-mix:* +eval +script +zadd +zrem +zremrangebyscore +zrangebyscore +zrange +sadd +expire +exists +set +get +hincrby +hgetall +hdel +ping +hello\n'
    ensure('database','redis-acl',{'users.acl':acl},'infrastructure/database/redis-acl-sealed.yaml')
    # The broker password file must contain every client. pi-mix and cudy are
    # managed here; any other users already present are retained as-is so a
    # rerun cannot silently drop an account again.
    broker={mqtt['username']:mqtt['password']}
    cudy=read_secret('cudy','cudy-collector')
    cudy_user=cudy.get('MQTT_USERNAME')
    if cudy_user and cudy.get('MQTT_PASSWORD'):
        broker[cudy_user]=cudy['MQTT_PASSWORD']
    retained={}
    for line in read_secret('iot','mosquitto-auth').get('passwords','').splitlines():
        user,sep,hashed=line.partition(':')
        if sep and user and hashed and user not in broker:
            retained[user]=hashed
    script='umask 077; f=$(mktemp); trap \'rm -f "$f"\' EXIT; cat > "$f"; mosquitto_passwd -U "$f"; cat "$f"'
    plain=''.join(user+':'+password+'\n' for user,password in broker.items())
    hashed=remote(['kubectl','exec','-i','-n','iot','deployment/mosquitto','--','sh','-c',script],plain.encode()).decode()
    passwordfile=''.join(line+'\n' for line in hashed.splitlines()+[u+':'+h for u,h in retained.items()])
    ensure('iot','mosquitto-auth',{'passwords':passwordfile},'k8s/mosquitto/auth-sealed.yaml',reuse=False)

if __name__=='__main__':main()
