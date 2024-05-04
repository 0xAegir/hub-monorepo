import { getLinksByFidInBatchesOf } from "../hub.js";
import { registerJob } from "../jobs.js";
import { mergeMessage } from "../processors/index.js";
import { executeTx } from "../db.js";

const MAX_PAGE_SIZE = 1_000;

export const BackfillFidLinks = registerJob({
  name: "BackfillFidLinks",
  run: async ({ fids }: { fids: number[] }, { db, log, redis, hub }) => {
    for (const fid of fids) {
      for await (const messages of getLinksByFidInBatchesOf(hub, fid, MAX_PAGE_SIZE)) {
        await executeTx(db, async (trx) => {
          for (const message of messages) {
            await mergeMessage(message, trx, log, redis);
          }
        });
      }
    }
    await redis.sadd("backfilled-links", ...fids);
  },
});
