# Fixture tenant Dockerfile for epic #841 item 18 tests — java8 base with a
# JDK 11 toolchain RUN inside the toolchain fence (D-841-019's fixture shape).
# operandai:fence:base:begin
FROM ghcr.io/operandai/hiops/worker-base-java8-gradle:v1.0.0
# operandai:fence:base:end

# operandai:fence:toolchain:begin
RUN apt-get install -y temurin-11-jdk
# operandai:fence:toolchain:end

CMD ["./entrypoint.sh"]
