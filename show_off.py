import asyncio
import json
from threading import Thread
import time

import psycopg

from pg_queue import worker


CONNECTION='host=localhost dbname=postgres user=postgres password=password'


def create_tables():
    with psycopg.connect(CONNECTION) as conn:
        cur = conn.cursor()
        cur.execute("""
        create table if not exists tasks
        ( task_id bigint primary key not null generated always as identity
        , task_type text not null -- consider using enum
        , params jsonb not null -- hstore also viable
        , created_at timestamptz not null default now()
        , unique (task_type, params) -- optional, for pseudo-idempotency
        )
        """)

        cur.execute("""
        create table if not exists users
        ( id bigint primary key not null generated always as identity
        , username text not null
        , password text not null
        )
        """)


def sample_event(username='hello'):
    with psycopg.connect(CONNECTION, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("""
        with users_ as (
        insert into users (username, password)
        values (%(username)s, %(password)s)
        returning *
        ), tasks_ as (
        insert into tasks (task_type, params)
        values ('print_pretty', %(params)s)
        )
        select * from users_
        """, {
            'username': username,
            'password': 'test',
            'params': json.dumps({'username': username}),
        })


async def create_events():
    from uuid import uuid4
    while True:
        sample_event(username=str(uuid4()))
        await asyncio.sleep(2)


async def main():
    worker_t = asyncio.create_task(worker.run())
    inserter_t = asyncio.create_task(create_events())

    await asyncio.gather(worker_t, inserter_t)

if __name__ == '__main__':
    create_tables()
    asyncio.run(main())
