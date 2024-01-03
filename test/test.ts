/*
 * test.ts
 *
 * Copyright (c) 2023-2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/pgqueue.js
 */

import { PgQueue } from "../src/index.js";

const pgUrl = "postgres://postgres:123456@127.0.0.1:5432/pqtest";

type Message = { code: number; payload: string };

export const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

const test = async () => {
  const pq = await PgQueue.acquire("test", pgUrl);

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const timestamp = new Date().getTime();
    const message = { code, payload: `Time: ${timestamp}` } as Message;
    await pq.produce([message]);
    console.log("PgQueue producer", message);
  }

  for (;;) {
    const message = await pq.consume<Message>(5);
    if (!message) break;
    console.log("PgQueue consumer", message);
  }

  await sleep(5000);

  for (;;) {
    const message = await pq.consume<Message>(5);
    if (!message) break;
    await pq.ack(message[0]);
    console.log("PgQueue consumer and ack", message);
  }

  pq.close();
};

test();
