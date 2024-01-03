/*
 * pgqueue.ts
 *
 * Copyright (c) 2023-2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/pgqueue.js
 */

import pg from "pg";

pg.types.setTypeParser(pg.types.builtins.NUMERIC, Number);
pg.types.setTypeParser(pg.types.builtins.INT8, BigInt);

import { default as knex } from "knex";
import { ulid } from "ulid";

export type PgSQLType = ReturnType<typeof knex>;

export type PgMessage = {
  id: string;
  payload: unknown;
  created_at: Date;
};

export class PgQueue {
  private readonly pgsql: PgSQLType;
  private readonly maintenance: boolean;
  private readonly name: string;

  private constructor(name: string, pgsql: PgSQLType, maintenance: boolean) {
    this.name = name;
    this.pgsql = pgsql;
    this.maintenance = maintenance;
  }

  private static createPgSQL(pgsqlOrUrl: PgSQLType | string) {
    const maintenance = typeof pgsqlOrUrl === "string";
    const pgsql = maintenance
      ? knex({
          client: "pg",
          connection: pgsqlOrUrl,
          pool: { min: 0, max: 10 },
        })
      : pgsqlOrUrl;

    return { maintenance, pgsql };
  }

  static async acquire(name: string, pgsqlOrUrl: PgSQLType | string) {
    const tableName = `pq_${name}`;
    const conn = PgQueue.createPgSQL(pgsqlOrUrl);

    if (!(await conn.pgsql.schema.hasTable(tableName))) {
      await conn.pgsql.schema.createTableLike(
        tableName,
        "pgqueue_table",
        (table) => {
          table.inherits("pgqueue_table");
        },
      );
    }

    return new PgQueue(name, conn.pgsql, conn.maintenance);
  }

  static async drop(names: string[], pgsqlOrUrl: PgSQLType | string) {
    const conn = PgQueue.createPgSQL(pgsqlOrUrl);

    await Promise.all(
      names.map((name) => conn.pgsql.schema.dropTableIfExists(`pq_${name}`)),
    );

    if (conn.maintenance) {
      await conn.pgsql.destroy();
    }
  }

  async close() {
    if (this.maintenance) {
      await this.pgsql.destroy();
    }
  }

  async produce<T>(payloads: T[]) {
    await this.pgsql<PgMessage>(`pq_${this.name}`).insert(
      payloads.map((payload) => ({
        id: ulid(),
        payload: JSON.stringify(payload),
      })),
    );

    return this;
  }

  async consume<T>(seconds: number) {
    const tableName = `pq_${this.name}`;

    return this.pgsql.transaction(async (trx) => {
      const row = await trx<PgMessage & { visible_at: Date }>(tableName)
        .where(
          "id",
          trx<PgMessage>(tableName)
            .select("id")
            .where("visible_at", "<", trx.fn.now())
            .orderBy("created_at", "asc")
            .limit(1)
            .forUpdate()
            .skipLocked(),
        )
        .update(
          "visible_at",
          trx.raw(`CURRENT_TIMESTAMP + INTERVAL '${seconds} seconds'`),
          "*",
        );

      if (row.length === 0) {
        return undefined;
      }

      return [row[0].id, row[0].payload] as [string, T];
    });
  }

  async ack(id: string) {
    await this.pgsql<PgMessage>(`pq_${this.name}`).where({ id }).delete();

    return this;
  }
}
