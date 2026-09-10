# Fixture tenant Dockerfile for epic #841 — java8 base with a JDK 11 toolchain
# RUN inside the toolchain fence (D-841-019's fixture shape).
#
# The base points at the compose registry, not ghcr: the operator's own token
# gets 403 Forbidden on ghcr.io/operandai/hiops/worker-base-java8-gradle, so
# that image is unreachable from this stack by authorization, not by login
# (L-490). `registry:5000/hiops/worker-base[-variant]:<tag>` is a form
# prepare_source's FROM_ALLOWLIST explicitly permits for local dev, and it
# keeps the `-java8-gradle` suffix so worker_profile still resolves to
# java8-gradle — item 14's fixture shape is unchanged.
# operandai:fence:base:begin
FROM registry:5000/hiops/worker-base-java8-gradle:vlocal-dev
# operandai:fence:base:end

# operandai:fence:toolchain:begin
# The base runs as USER worker, so apt needs root; and the Adoptium repo
# index has to be fetched before temurin-11-jdk resolves. Without both,
# this RUN exits 100 — which it did, because this fence had never been built.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends temurin-11-jdk \
 && rm -rf /var/lib/apt/lists/*
USER worker
# operandai:fence:toolchain:end

CMD ["./entrypoint.sh"]
