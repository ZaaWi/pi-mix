#!/usr/bin/env python3
"""Stage scoped credentials and write only SealedSecrets to the repository.
Run locally with --cert and --kubeseal. Existing cluster credentials are reused.
This does not switch broker authentication or modify PostgreSQL roles.
"""
import argparse, base64, hashlib, json, pathlib, secrets, subprocess


def remote(args, data=None):
    import shlex
    return subprocess.check_output(['ssh','the-bear','sudo -n '+shlex.join(args)],input=data)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--cert',required=True)
    parser.add_argument('--kubeseal',required=True)
    args=parser.parse_args()
    def ensure(namespace,name,values,path):
        raw=remote(['kubectl','get','secret',name,'-n',namespace,'--ignore-not-found','-o','json'])
        if raw.strip():
            obj=json.loads(raw)
            values={k:base64.b64decode(v).decode() for k,v in obj['data'].items()}
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
    acl='user default off\nuser admin on #'+hashlib.sha256(admin['password'].encode()).hexdigest()+' ~* &* +@all\nuser pi-mix on #'+hashlib.sha256(cache['password'].encode()).hexdigest()+' ~pi-mix:* +eval +script +zadd +zremrangebyscore +zrangebyscore +zrange +sadd +expire +exists +set +get +hincrby +hgetall +hdel +ping +hello\n'
    ensure('database','redis-acl',{'users.acl':acl},'infrastructure/database/redis-acl-sealed.yaml')
    script='umask 077; f=$(mktemp); trap \'rm -f "$f"\' EXIT; cat > "$f"; mosquitto_passwd -U "$f"; cat "$f"'
    passwordfile=remote(['kubectl','exec','-i','-n','iot','deployment/mosquitto','--','sh','-c',script],(mqtt['username']+':'+mqtt['password']+'\n').encode()).decode()
    ensure('iot','mosquitto-auth',{'passwords':passwordfile},'k8s/mosquitto/auth-sealed.yaml')

if __name__=='__main__':main()
