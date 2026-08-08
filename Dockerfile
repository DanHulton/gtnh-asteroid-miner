FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    gnupg \
    git \
    nano \
    && mkdir -p /etc/apt/keyrings

RUN curl -fsSL https://omp.sh/install | sh

RUN export PATH=$PATH:/root/.local/bin

RUN export LM_STUDIO_BASE_URL=http://host.docker.internal:1234/v1

WORKDIR /workspace

CMD ["bash"]
