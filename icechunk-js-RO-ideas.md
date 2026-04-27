Feasibility confirmed. Here's the full picture:

Yes, totally doable. Probably ~100 lines of TS.

What maps cleanly:
Python catalog	icechunk-js equivalent
inspect_repo_info() → all branches+metadata	repo.listBranches() + repo.checkoutBranch(name) → session.getSnapshotMetadata()
branch name = entry name	listBranches() → filter out "main"
snapshot user metadata = entry fields	session.getSnapshotMetadata() — returns the key-value blob
entry.location	parsed from that metadata blob
Sketch of the JS catalog reader:

import { Repository, HttpStorage } from "icechunk-js";

export async function openCatalog(url: string) {
  const repo = await Repository.open({ storage: new HttpStorage(url) });
  const branches = await repo.listBranches();
  
  const entries = await Promise.all(
    branches
      .filter(b => b !== "main")
      .map(async (name) => {
        const session = await repo.checkoutBranch(name);
        const meta = session.getSnapshotMetadata(); // has location, owner, title, etc.
        return { name, ...meta };
      })
  );
  
  return entries;
}
One uncertainty:
session.getSnapshotMetadata() — the subagent found it referenced but didn't confirm the exact return type. Need to verify the metadata blob is the same user-metadata dict that Python writes (it should be — icechunk-js reads the same format file). Worth checking one real call against the demo catalog at s3://carbonplan-share/ic-catalog/_catalog via HTTP endpoint.

Demo catalog access:
That bucket is public. The HTTP URL would be something like https://carbonplan-share.s3.us-west-2.amazonaws.com/ic-catalog/_catalog — icechunk-js HttpStorage handles S3 public buckets fine via HTTPS.

Bottom line: listBranches + checkoutBranch + getSnapshotMetadata per branch = full catalog read. No CRUD needed, this is all read-only. The zarrita integration for actually opening datasets already works since entry.location gives you the store URL.