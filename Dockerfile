# Fixture tenant Dockerfile for epic #841 — java8 base with a JDK 11 toolchain
# RUN inside the toolchain fence (D-841-019's fixture shape).
#
# The base points at ghcr's stand-in in the LOCAL registry: the operator's own
# token gets 403 Forbidden on ghcr.io/operandai/hiops/worker-base-java8-gradle,
# so that image is unreachable from this stack by authorization, not by login
# (L-490). The `-java8-gradle` suffix is kept so worker_profile still resolves
# to java8-gradle — item 14's fixture shape is unchanged.
#
# WHY `localhost:5555` AND NOT `registry:5000` — and what it costs.
# Two different daemons build this file and they do not share a resolver:
#   * the toolchain-amendment VERIFICATION build is a plain `docker build`
#     subprocess (toolchain_amendment_activities._run_local_docker_build) on the
#     platform worker, which reaches the HOST daemon through the mounted
#     socket. The host has no compose DNS, so `registry:5000` fails with
#     `dial tcp: lookup registry: no such host` — measured, not assumed.
#     `localhost:5555` is the registry's published host port and resolves.
#   * the real TENANT IMAGE build runs in buildkitd, a container ON the compose
#     network, where the reverse is true: `registry:5000` resolves and
#     `localhost:5555` is buildkitd itself.
# No single string satisfies both, so this fixture picks the one the amendment
# path needs. The cost is real and is not hidden: with this FROM, a tenant image
# build of this repo stops at prepare_source's FROM_ALLOWLIST, which permits
# `registry:<port>/hiops` and not `localhost:<port>/hiops`. That allowlist is a
# security property and was deliberately left alone rather than widened for a
# fixture's convenience.
# operandai:fence:base:begin
FROM localhost:5555/hiops/worker-base-java8-gradle:vlocal-dev
# operandai:fence:base:end

# operandai:fence:toolchain:begin
# The base runs as USER worker, so apt needs root; and the Adoptium repo
# index has to be fetched before temurin-17-jdk resolves. Without both,
# this RUN exits 100 — which it did, because this fence had never been built.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends temurin-17-jdk \
 && rm -rf /var/lib/apt/lists/*
USER worker
# operandai:fence:toolchain:end

# No CMD override: the base image already ends with
#   CMD ["python", "-m", "app.temporal.unified_worker"]
# The previous `CMD ["./entrypoint.sh"]` pointed at the base's DEV entrypoint,
# which pip-installs -e /platform_lib — a path only the compose services bind-mount,
# never a tenant container started by _deploy_local (deploy.py mounts skills and
# workspaces only). That made every deployed tenant container crash-loop.
