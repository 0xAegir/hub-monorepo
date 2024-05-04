import { getUserNameProofsByFid } from "../hub.js";
import { registerJob } from "../jobs.js";
import { processUserNameProof } from "../processors/index.js";
import { BackfillFidUserData } from "./backfillFidUserData.js";

export const BackfillFidUserNameProofs = registerJob({
  name: "BackfillFidUserNameProofs",
  run: async ({ fids }: { fids: number[] }, { db, log, redis, hub }) => {
    for (const fid of fids) {
      // Username proofs don't always have a corresponding protocol message (e.g. fnames),
      // and so need to be synced directly
      for (const proof of await getUserNameProofsByFid(hub, fid)) {
        await processUserNameProof(db, log, proof);
      }
    }
    await redis.sadd("backfilled-username-proofs", ...fids);

    // Now that the username proof is backfilled, we can process the user data that references it
    await BackfillFidUserData.enqueue({ fids });
  },
});
