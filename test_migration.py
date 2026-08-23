import asyncio
import asyncpg
import os

DATABASE_URL = "postgresql://neondb_owner:npg_KRIaPCS8VW2k@ep-wild-cherry-a4f3j2pw-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

async def test_migration():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        with open("migrations/001_initial_schema.sql", 'r') as f:
            sql = f.read()
        print("Executing migration...")
        await conn.execute(sql)
        print("Success!")
    except Exception as e:
        print(f"Failed: {e}")
    finally:
        await conn.close()

asyncio.run(test_migration())
