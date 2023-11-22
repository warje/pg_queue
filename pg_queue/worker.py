import asyncio
import logging

import psycopg

from pg_queue.tasks import ALL_TASKS

CONNECTION='host=localhost dbname=postgres user=postgres password=password'

logger = logging.getLogger()


async def handler(conn):
    async with conn.transaction() as tx:
        async with conn.cursor() as cur:
            await cur.execute("""
            delete from tasks
            where task_id in
            ( select task_id
              from tasks
              order by random()
              for update
              skip locked
              limit 1
            )
            returning task_id, task_type, params::jsonb as params
            """)

            rows = await cur.fetchall()
            if not rows:
                return True

            for _, task_type, params in rows:
                if task_type in ALL_TASKS:
                    await ALL_TASKS[task_type](tx, params)


async def run():
    async with await psycopg.AsyncConnection.connect(CONNECTION) as conn:
        while True:
            try:
                backoff = await handler(conn)
                if backoff:
                    await asyncio.sleep(5)

            except Exception as err:
                logger.exception('Failure executing tasks')
