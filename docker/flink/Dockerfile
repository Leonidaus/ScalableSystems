FROM flink:2.3.0-java17

# Remove the Scala 2.12 stdlib jar from flink, as it is incompatible with Scala 2.13.
RUN rm -f /opt/flink/lib/flink-scala_2.12-*.jar
