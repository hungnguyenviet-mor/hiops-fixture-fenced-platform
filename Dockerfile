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
RUN apt-get install -y temurin-11-jdk
# operandai:fence:toolchain:end

CMD ["./entrypoint.sh"]
