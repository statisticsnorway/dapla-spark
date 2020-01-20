module dapla.spark.service {
    requires dapla.spark.protobuf;
    requires no.ssb.dapla.catalog.protobuf;
    requires no.ssb.helidon.media.protobuf.json.server;
    requires no.ssb.dapla.auth.dataset.protobuf;
    requires org.slf4j;
    requires jul.to.slf4j;
    requires org.reactivestreams;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.core;
    requires io.helidon.webserver;
    requires io.helidon.config;
    requires java.net.http;
    requires io.helidon.common.reactive;
    requires logback.classic;
    requires io.helidon.metrics;
    requires io.helidon.health;
    requires io.helidon.health.checks;
    requires io.grpc;

    requires grpc.protobuf;
    requires io.helidon.grpc.server;
    requires java.logging;

    requires com.google.gson; // required by JsonFormat in protobuf-java-util for serialization and deserialization

    requires java.annotation;
    requires com.google.protobuf.util;
    requires com.google.common;
    requires no.ssb.helidon.application;

    /*
     * Not so well documented requirements are declared here to force fail-fast with proper error message if
     * missing from jvm.
     */
    requires jdk.unsupported; // required by netty to allow reliable low-level API access to direct-buffers
    requires jdk.naming.dns; // required by netty dns libraries used by reactive postgres
    requires java.sql; // required by flyway
    requires io.helidon.microprofile.config; // metrics uses provider org.eclipse.microprofile.config.spi.ConfigProviderResolver
    requires perfmark.api; // needed by grpc-client
    requires javax.inject; // required by io.helidon.grpc.server

    requires org.checkerframework.checker.qual;

    provides no.ssb.helidon.application.HelidonApplicationBuilder with no.ssb.dapla.spark.service.ApplicationBuilder;
}