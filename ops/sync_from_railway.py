#!/usr/bin/env python3
"""Copy a Railway Postgres into its new home on relay 242, DB to DB, re-runnable (Railway -> 242 move, 2026-11-18).

Run on a machine that can `ssh mikeoslaptop` (space-computer, where the railway-agent container lives) and
`ssh root@91.98.177.242` (key ~/.ssh/mikeos_media), e.g. latidude:

    python3 ops/sync_from_railway.py ops/railway_sync.json            # copy + merge, prints counts per table
    python3 ops/sync_from_railway.py ops/railway_sync.json --dry-run  # counts only, writes nothing

How it works (rows are never printed or written to disk; they stream through two ssh pipes):
  * Railway side: a temporary SSH key is registered with Railway (name --key-name), `railway ssh` into the project's
    Postgres service runs `COPY (SELECT row_to_json(x)) TO STDOUT` per table, gzip+base64 for a clean pipe. The key is
    removed again at the end (also on errors) and its files are deleted. Railway itself is only read.
  * 242 side: `docker exec -i <db container> psql` loads the rows into a temp table and merges them into the target
    table with `json_populate_record`, by primary key:
      policy "insert"      new primary keys are inserted, existing rows are left alone (242 is the live copy)
      policy "newer:<col>" like insert, and an existing row is replaced when Railway's <col> is newer
      policy "newer:<col>:<a>,<b>"  the same, but only columns a and b are taken over
    Serial sequences of the target table are moved past the highest id afterwards.
    A table may set "dedupe_on": ["id"] (a unique column other than the primary key) plus "drop": ["seq"]: rows are
    matched on that column (with policy "insert": on ANY unique column, e.g. also client_msg_id) and the dropped serial column is given fresh by 242 (no clash with rows 242 made itself).
  * Optional "post_sql" (a file next to the config) runs on the target after all tables, e.g. to transform staged
    rows into another service's schema.

Config (JSON): {"service", "railway": {"project", "environment", "postgres_service", "database"},
                "target": {"container", "user", "database"},
                "tables": [{"src": "schema.table", "dst": "schema.table", "policy": "insert", "dedupe_on": [..], "drop": [..]}],
                "pre_sql": "file.sql", "user_map": {...}, "post_sql": "file.sql"}
"""
import argparse
import base64
import json
import os
import shlex
import subprocess
import sys
import time

SPACE = os.environ.get("RAILWAY_SYNC_SPACE", "mikeoslaptop")
R242 = os.environ.get("RAILWAY_SYNC_242", "root@91.98.177.242")
R242_KEY = os.path.expanduser(os.environ.get("RAILWAY_SYNC_242_KEY", "~/.ssh/mikeos_media"))
AGENT = "railway-agent"        # its image, env file (a2a key -> RAILWAY_API_TOKEN in secretz) and add-hosts are reused

# Runs inside the railway-agent container: the Railway CLI with a private HOME, a wrapper `ssh` (the container's
# /root/.ssh is a read-only mount) and the workspace token fetched from secretz with the container's own a2a key.
RW_HELPER = r'''#!/bin/sh
export HOME=/tmp/rh-$RWKEY
mkdir -p $HOME/bin $HOME/.ssh
[ -f $HOME/.ssh/id ] || ssh-keygen -q -t ed25519 -N "" -f $HOME/.ssh/id -C $RWKEY
printf 'Host *\n  StrictHostKeyChecking accept-new\n  UserKnownHostsFile %s/.ssh/known_hosts\n  IdentityFile %s/.ssh/id\n  IdentitiesOnly yes\n' $HOME $HOME > $HOME/.ssh/config
printf '#!/bin/sh\nexec /usr/bin/ssh -F %s/.ssh/config -i %s/.ssh/id "$@"\n' $HOME $HOME > $HOME/bin/ssh
chmod +x $HOME/bin/ssh
export PATH=$HOME/bin:$PATH
export RAILWAY_API_TOKEN=$(python3 -c '
import json,urllib.request
env=dict(x.decode().split("=",1) for x in open("/proc/1/environ","rb").read().split(b"\0") if b"=" in x)
r=urllib.request.Request("https://secretz-api.osmike.com/v1/secrets/RAILWAY_API_TOKEN",headers={"X-API-KEY":env["A2A_AGENT_KEY"],"User-Agent":"Mozilla/5.0"})
print(json.load(urllib.request.urlopen(r))["value"])')
exec npx -y @railway/cli@latest "$@"
'''

QUOTE = "e'\\x01'"
DELIM = "e'\\x02'"


def run(cmd, inp=None, check=True):
    p = subprocess.run(cmd, input=inp, capture_output=True)
    if check and p.returncode != 0:
        raise SystemExit("command failed (%s): %s" % (p.returncode, p.stderr.decode(errors="replace")[-800:]))
    return p.stdout


def space(sh, inp=None, check=True):
    return run(["ssh", "-o", "ConnectTimeout=20", SPACE, sh], inp, check)


def r242(sh, inp=None, check=True):
    return run(["ssh", "-o", "ConnectTimeout=20", "-i", R242_KEY, R242, sh], inp, check)


class Railway:
    """A throwaway copy of the railway-agent container (same image, env file and hosts; `sleep`, so no agent loop
    starts and the box's reaper leaves it alone). Removed again in teardown."""

    def __init__(self, cfg, key_name):
        self.cfg, self.key = cfg, key_name
        self.box = "rwsync-" + key_name
        self.helper = "/tmp/rw.sh"

    def cli(self, args, check=True):
        cmd = "docker exec -e RWKEY=%s %s sh %s %s" % (self.key, self.box, self.helper, args)
        return space(cmd, check=check)

    def setup(self):
        hosts = json.loads(space("docker inspect %s --format '{{json .HostConfig.ExtraHosts}}'" % AGENT).decode() or "[]")
        space("docker rm -f %s >/dev/null 2>&1; docker run -d --rm --name %s --label osmike.role=railway-sync "
              "--env-file $HOME/agents/%s/.env %s --entrypoint sleep %s:latest 7200 >/dev/null"
              % (self.box, self.box, AGENT, " ".join("--add-host " + shlex.quote(h) for h in hosts or []), AGENT))
        space("docker exec -i %s sh -c 'cat > %s'" % (self.box, self.helper), RW_HELPER.encode())
        # the helper creates the key files on its first run; then register the public key once
        if not self.registered():
            self.cli("ssh keys add -k /tmp/rh-%s/.ssh/id.pub -n %s" % (self.key, self.key))

    def registered(self):
        """Is our key in the "Registered SSH Keys" part of the listing (the listing also shows local, unregistered keys)."""
        out = self.cli("ssh keys list", check=False).decode(errors="replace")
        reg = out.split("Local Keys")[0]
        return ("  %s\n" % self.key) in reg.replace("\r", "")

    def teardown(self):
        try:
            fp = space("docker exec %s ssh-keygen -lf /tmp/rh-%s/.ssh/id.pub" % (self.box, self.key), check=False).decode().split()
            if len(fp) > 1:
                self.cli("ssh keys remove %s" % fp[1], check=False)
            if self.registered():
                print("WARNING: Railway SSH key %s is still registered; remove it by hand" % self.key, file=sys.stderr)
        finally:
            space("docker rm -f %s >/dev/null 2>&1" % self.box, check=False)

    def pg(self, shell):
        r = self.cfg["railway"]
        b = base64.b64encode(shell.encode()).decode()
        args = "ssh -p %s -e %s -s %s -- %s" % (r["project"], r["environment"], shlex.quote(r["postgres_service"]),
                                              shlex.quote("echo %s | base64 -d | sh" % b))
        out = self.cli(args).decode(errors="replace")
        lines = [l.strip() for l in out.replace("\r", "").split("\n")]
        return [l for l in lines if l and not l.startswith("npm") and not l.startswith("Using SSH key")
                and "Permanently added" not in l]

    def psql(self, sql):
        db = self.cfg["railway"].get("database", "railway")
        return "psql -U postgres -d %s -X -At -v ON_ERROR_STOP=1 -c %s" % (db, shlex.quote(sql))

    def count(self, table):
        out = self.pg(self.psql("select 'N=' || count(*) from %s" % table))
        return int([l for l in out if l.startswith("N=")][-1][2:])

    def export(self, table):
        sql = ("copy (select row_to_json(x)::text from %s x) to stdout with (format csv, quote %s, delimiter %s)"
               % (table, QUOTE, DELIM))
        out = self.pg("echo BEGIN64; %s | gzip -c | base64 -w0; echo; echo END64" % self.psql(sql))
        i, j = out.index("BEGIN64"), out.index("END64")
        data = "".join(out[i + 1:j])
        return __import__("gzip").decompress(base64.b64decode(data)) if data else b""


class Target:
    def __init__(self, cfg):
        self.t = cfg["target"]

    def psql(self, sql_stdin):
        t = self.t
        return r242("docker exec -i %s psql -U %s -d %s -X -At -v ON_ERROR_STOP=1" % (t["container"], t["user"], t["database"]),
                    sql_stdin).decode(errors="replace")

    def count(self, table):
        return int(self.psql(("select count(*) from %s;" % table).encode()).strip().split("\n")[-1])

    def pk(self, table):
        out = self.psql(("""select a.attname from pg_index i join pg_attribute a on a.attrelid=i.indrelid
            and a.attnum = any(i.indkey) where i.indrelid = '%s'::regclass and i.indisprimary order by a.attnum;""" % table).encode())
        return [x for x in out.strip().split("\n") if x]

    def cols(self, table):
        s, n = table.split(".")
        out = self.psql(("select column_name from information_schema.columns where table_schema='%s' and table_name='%s' "
                         "order by ordinal_position;" % (s, n)).encode())
        return [x for x in out.strip().split("\n") if x]

    def serial_cols(self, table, cols):
        sql = " union all ".join("select '%s' where pg_get_serial_sequence('%s','%s') is not null" % (c, table, c) for c in cols)
        return [x for x in self.psql((sql + ";").encode()).strip().split("\n") if x]

    def merge(self, table, policy, rows, dry, dedupe_on=None, drop=()):
        pk = self.pk(table)
        if not pk:
            raise SystemExit("%s has no primary key on 242; refusing to merge" % table)
        cols = self.cols(table)
        q = lambda c: '"%s"' % c
        keep = [c for c in cols if c not in drop]
        rec = "(json_populate_record(null::%s, j)).*" % table
        if dedupe_on:      # e.g. messages: match on the message uuid, let 242 give a fresh serial seq
            pk = list(dedupe_on)
            rec = ", ".join("(r).%s" % q(c) for c in keep)
        if policy == "insert":
            conflict = "do nothing"
        elif policy.startswith("newer:"):
            parts = policy.split(":")
            ts = parts[1]
            upd = [c for c in cols if c not in pk]
            if len(parts) > 2:                      # newer:<ts>:<col>,<col>  only these columns
                upd = [c for c in parts[2].split(",") if c in cols]
            conflict = "do update set %s where excluded.%s > _t.%s" % (
                ", ".join("%s = excluded.%s" % (q(c), q(c)) for c in upd), q(ts), q(ts))
        else:
            raise SystemExit("unknown policy %s" % policy)
        sql = ["\\set ON_ERROR_STOP 1", "begin;", "create temp table _in (j json) on commit drop;",
               "copy _in from stdin with (format csv, quote %s, delimiter %s);" % (QUOTE, DELIM)]
        body = "\n".join(sql).encode() + b"\n" + rows + (b"" if rows.endswith(b"\n") or not rows else b"\n") + b"\\.\n"
        tail = ["select 'IN=' || count(*) from _in;",
                "with ins as (insert into %s as _t %s select %s from %s on conflict %s %s returning (xmax = 0) as inserted) "
                "select 'NEW=' || count(*) filter (where inserted) || ' UPD=' || count(*) filter (where not inserted) from ins;"
                % (table, "(%s)" % ",".join(map(q, keep)) if dedupe_on else "", rec,
                   "(select json_populate_record(null::%s, j) as r from _in) _s" % table if dedupe_on else "_in",
                   "" if (dedupe_on and policy == "insert") else "(%s)" % ",".join(map(q, pk)), conflict)]
        # serial sequences behind the copied ids
        for c in self.serial_cols(table, cols):
            tail.append("select setval(pg_get_serial_sequence('%s','%s'), greatest((select max(%s) from %s), 1));"
                        % (table, c, q(c), table))
        tail.append("rollback;" if dry else "commit;")
        out = self.psql(body + "\n".join(tail).encode())
        res = {}
        for l in out.split("\n"):
            for part in l.split():
                if "=" in part and part.split("=")[0] in ("IN", "NEW", "UPD"):
                    res[part.split("=")[0]] = int(part.split("=")[1])
        return res


def map_users(um, tg, dry):
    """Old account user ids -> e-mail, by EXACT id in the account database (never guessed). um = {"ids": SQL on the
    target giving user ids, "account": {container, user, database}, "extra_ids": SQL on the account DB (optional),
    "into": table(user_id, email, source)}. Ids without an account row are reported, not mapped."""
    ids = [x for x in tg.psql((um["ids"] + ";").encode()).strip().split("\n") if x]
    acc = um["account"]
    lst = ",".join("'%s'" % i.replace("'", "") for i in ids) or "NULL"
    sql = "select id::text || chr(9) || lower(email) from users where email is not null and (id::text in (%s)%s);" % (
        lst, (" or id in (%s)" % um["extra_ids"]) if um.get("extra_ids") else "")
    rows = r242("docker exec -i %s psql -U %s -d %s -X -At -v ON_ERROR_STOP=1" % (acc["container"], acc["user"], acc["database"]),
                sql.encode()).decode()
    pairs = [l.split("\t") for l in rows.strip().split("\n") if "\t" in l]
    found = {p[0] for p in pairs}
    vals = ",".join("('%s','%s','account-db')" % (u.replace("'", ""), e.replace("'", "")) for u, e in pairs)
    if vals:
        tg.psql(("%s insert into %s(user_id, email, source) values %s on conflict (user_id) do update set email = excluded.email; %s"
                 % ("begin;" if dry else "", um["into"], vals, "rollback;" if dry else "")).encode())
    missing = [i for i in ids if i not in found]
    print("user ids: %d in the copied rows, %d mapped to an account e-mail (%d mappings written incl. app users), unmapped: %s"
          % (len(ids), len([i for i in ids if i in found]), len(pairs), ", ".join(missing) or "none"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("config")
    ap.add_argument("--dry-run", action="store_true", help="count and merge inside a transaction that is rolled back")
    ap.add_argument("--key-name", default=None, help="name of the temporary Railway SSH key (unique per run)")
    a = ap.parse_args()
    cfg = json.load(open(a.config))
    key = a.key_name or "rwsync-%s-%d" % (cfg["service"], os.getpid())
    rw, tg = Railway(cfg, key), Target(cfg)
    report = []
    try:
        rw.setup()
        here = os.path.dirname(os.path.abspath(a.config))
        if cfg.get("pre_sql"):                  # e.g. staging tables for rows that are transformed afterwards
            tg.psql(open(os.path.join(here, cfg["pre_sql"]), "rb").read())
        for t in cfg["tables"]:
            src, dst, pol = t["src"], t.get("dst", t["src"]), t.get("policy", "insert")
            before = tg.count(dst)
            n_src = rw.count(src)
            rows = rw.export(src)
            n_rows = rows.count(b"\n") + (1 if rows and not rows.endswith(b"\n") else 0)
            if n_rows != n_src:
                raise SystemExit("%s: exported %d rows but Railway counts %d; stopping" % (src, n_rows, n_src))
            res = tg.merge(dst, pol, rows, a.dry_run, t.get("dedupe_on"), t.get("drop", ()))
            after = tg.count(dst)
            report.append((src, dst, pol, n_src, before, res.get("NEW", 0), res.get("UPD", 0), after))
            print("%-34s railway=%-6d 242 before=%-6d new=%-5d updated=%-5d after=%d%s" % (
                src + (" -> " + dst if dst != src else ""), n_src, before, res.get("NEW", 0), res.get("UPD", 0), after,
                " (dry run)" if a.dry_run else ""), flush=True)
        if cfg.get("user_map"):
            map_users(cfg["user_map"], tg, a.dry_run)
        if cfg.get("post_sql") and a.dry_run:
            print("post_sql %s skipped (dry run)" % cfg["post_sql"])
        elif cfg.get("post_sql"):
            out = tg.psql(open(os.path.join(here, cfg["post_sql"]), "rb").read())
            for l in out.split("\n"):
                if l.startswith("REPORT "):
                    print(l[7:])
    finally:
        rw.teardown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
