# Cluster Spec (Community Edition)

Document your CE cluster so reviewers can reproduce.

- **Runtime:** 13.x / 14.x Photon (pick available CE runtime)
- **Node type:** Single-node (CE default, no autoscaling)
- **Driver:** Standard_F4 or equivalent (4 cores, 16GB) — adjust to CE offering
- **Auto-termination:** 60 min (CE default). Workaround: `trigger(availableNow=True)` micro-batches instead of continuous stream. Production: continuous trigger under Databricks Workflows on an always-on job cluster.
- **Libraries:** Installed via notebook `%pip install -r requirements.txt` where `requirements.txt` is `uv export --no-hashes -o requirements.txt` from `uv.lock`.

Capture a screenshot of Cluster > Configuration and link here.
