# syntax=docker/dockerfile:1

# --- Stage 1: sbt build ------------------------------------------------------
# Produces the fat JAR inside Docker, so the host needs no JDK or sbt.
FROM eclipse-temurin:17-jdk AS build

RUN curl -fsSL https://github.com/sbt/sbt/releases/download/v1.10.1/sbt-1.10.1.tgz \
      | tar -xz -C /opt \
  && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

WORKDIR /build

# Copy the build definition first so dependency resolution is cached
# independently of source changes.
COPY build.sbt ./
COPY project/ project/
COPY src/ src/

# Cache mounts keep sbt/coursier artifacts between builds.
RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    sbt --batch assembly

# --- Stage 2: Flink runtime --------------------------------------------------
# No published Flink image is Scala-free — not even in 2.x, where flink-scala
# is nothing but the Scala 2.12 standard library. Flink resolves scala.*
# parent-first, so those classes shadow the 2.13 library in our JAR and the
# job dies at submission with NoSuchMethodError: scala.Predef$.refArrayOps.
FROM flink:2.3.0-java17
RUN rm -f /opt/flink/lib/flink-scala_2.12-*.jar
COPY --from=build /build/target/scala-2.13/scalable-systems-0.1.0.jar /jobs/scalable-systems-0.1.0.jar
