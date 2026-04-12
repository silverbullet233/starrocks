# Use StarRocks development environment as base image
FROM 172.26.92.142:5000/starrocks/dev-env-ubuntu:latest

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Add Aliyun Ubuntu mirrors to apt sources
RUN echo "deb http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse" > /etc/apt/sources.list.d/aliyun.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse" >> /etc/apt/sources.list.d/aliyun.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse" >> /etc/apt/sources.list.d/aliyun.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse" >> /etc/apt/sources.list.d/aliyun.list

# Update package list and install initial dependencies
RUN apt-get update && apt-get install -y \
    lsb-release \
    wget \
    software-properties-common \
    gnupg \
    ninja-build \
    && rm -rf /var/lib/apt/lists/*

# Install LLVM 21 (latest stable version)
RUN wget https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    ./llvm.sh 21 && \
    rm llvm.sh

# Install additional LLVM tools including clang-tidy-21
RUN apt-get update && \
    apt-get install -y clang-tidy-21 && \
    rm -rf /var/lib/apt/lists/*

# Set LLVM 21 as default for all LLVM tools
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-21 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-21 100 && \
    update-alternatives --install /usr/bin/clangd clangd /usr/bin/clangd-21 100 && \
    update-alternatives --install /usr/bin/lld lld /usr/bin/lld-21 100 && \
    update-alternatives --install /usr/bin/llvm-ar llvm-ar /usr/bin/llvm-ar-21 100 && \
    update-alternatives --install /usr/bin/llvm-as llvm-as /usr/bin/llvm-as-21 100 && \
    update-alternatives --install /usr/bin/llvm-dis llvm-dis /usr/bin/llvm-dis-21 100 && \
    update-alternatives --install /usr/bin/llvm-link llvm-link /usr/bin/llvm-link-21 100

RUN ln -s /bin/clangd /usr/local/bin/clangd

RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt install -y nodejs && \
    npm i -g @openai/codex && \
    npm i -g @google/gemini-cli

# Set default environment variables
ENV CC=clang \
    CXX=clang++ \
    STARROCKS_GCC_HOME=/usr \
    https_proxy=http://0.0.0.0:7890 \
    http_proxy=http://0.0.0.0:7890

# Create user xujia with home directory /home/xujia and specific UID
RUN useradd -m -d /home/xujia -s /bin/bash -u 1005 xujia 

# Switch to xujia user
USER xujia

# Set default command
CMD ["/bin/bash"]
