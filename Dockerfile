ARG VERSION=4.0.10
FROM apachepulsar/pulsar:${VERSION}

ARG IMAGE_TITLE="pulsar-patched"
ARG IMAGE_DESCRIPTION="Apache Pulsar image with some patches"
ARG IMAGE_SOURCE="https://github.com/devngho/pulsar"
ARG IMAGE_REVISION="unknown"
ARG IMAGE_CREATED="unknown"

LABEL org.opencontainers.image.title="${IMAGE_TITLE}" \
      org.opencontainers.image.description="${IMAGE_DESCRIPTION}" \
      org.opencontainers.image.source="${IMAGE_SOURCE}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.created="${IMAGE_CREATED}"

COPY ./tiered-storage/*/build/libs/*.nar /pulsar/offloaders/
