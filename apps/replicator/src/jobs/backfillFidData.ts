import { OnChainEvent, OnChainEventType } from "@farcaster/hub-nodejs";
import { getOnChainEventsByFidInBatchesOf } from "../hub.js";
import { registerJob } from "../jobs.js";
import { processOnChainEvents } from "../processors/index.js";
import { BackfillFidCasts } from "./backfillFidCasts.js";
import { BackfillFidVerifications } from "./backfillFidVerifications.js";
import { BackfillFidLinks } from "./backfillFidLinks.js";
import { BackfillFidUserNameProofs } from "./backfillFidUserNameProofs.js";
import { BackfillFidOtherOnChainEvents } from "./backfillFidOtherOnChainEvents.js";
import { BackfillFidReactions } from "./backfillFidReactions.js";
import { BackfillFidStorageAllocations } from "./backfillFidStorageAllocations.js";

const MAX_PAGE_SIZE = 1_000;

export const BackfillFidData = registerJob({
  name: "BackfillFidData",
  run: async ({ fids }: { fids: number[] }, { db, log, redis, hub }) => {
    const alreadyBackfilledSigners = await redis.smismember("backfilled-signers", ...fids);
    if (fids.length !== alreadyBackfilledSigners.length) {
      throw new Error(`Got mismatched result length from smismember. Expected ${fids.length}, got ${alreadyBackfilledSigners.length}`);
    }

    await forRemaining(redis, "backfilled-signers", fids, async (fids) => {
      for (const fid of fids) {
        let signerEvents: OnChainEvent[] = [];
        for await (const events of getOnChainEventsByFidInBatchesOf(hub, {
          fid,
          pageSize: MAX_PAGE_SIZE,
          eventTypes: [OnChainEventType.EVENT_TYPE_SIGNER],
        })) {
          signerEvents = signerEvents.concat(...events);
        }

        // Since there could be many events, ensure we process them in sorted order
        const sortedEventsForFid = signerEvents.sort((a, b) =>
          a.blockNumber === b.blockNumber ? a.logIndex - b.logIndex : a.blockNumber - b.blockNumber,
        );
        await processOnChainEvents(sortedEventsForFid, db, log, redis);
      }
    });
    await redis.sadd("backfilled-signers", ...fids);

    // Now that this FID's signers are backfilled, we can start backfilling some message types
    await Promise.all([
      forRemaining(redis, "backfilled-casts", fids, (fids) => BackfillFidCasts.enqueue({ fids })),
      forRemaining(redis, "backfilled-links", fids, (fids) => BackfillFidLinks.enqueue({ fids })),
      forRemaining(redis, "backfilled-reactions", fids, (fids) => BackfillFidReactions.enqueue({ fids })),
      forRemaining(redis, "backfilled-verifications", fids, (fids) => BackfillFidVerifications.enqueue({ fids })),
      forRemaining(redis, "backfilled-username-proofs", fids, (fids) => BackfillFidUserNameProofs.enqueue({ fids })),
      forRemaining(redis, "backfilled-storage-allocations", fids, (fids) => BackfillFidStorageAllocations.enqueue({ fids })),
      forRemaining(redis, "backfilled-other-onchain-events", fids, (fids) => BackfillFidOtherOnChainEvents.enqueue({ fids })),
    ]);
  },
});

async function forRemaining(redis: any, set: string,  fids: number[], f: (fids: number[]) => Promise<any>) {
  const alreadyBackfilled = await redis.smismember(set, ...fids);
  const remaining = fids.filter((_, i) => !alreadyBackfilled[i]);
  await f(remaining);
}
