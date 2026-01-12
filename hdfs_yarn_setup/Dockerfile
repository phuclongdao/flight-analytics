FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# 1. Chạy với quyền root
USER root

# 2. Fix repository Debian Stretch đã cũ → dùng archive.debian.org
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list && \
    # Tắt kiểm tra "valid-until" của Stretch repo cũ
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99disable-check-valid-until && \
    # Cập nhật apt và cài Python3 + pip
    apt-get update && apt-get install -y python3 python3-pip && \
    # Cài package Python hdfs
    pip3 install --no-cache-dir hdfs && \
    # Dọn cache apt
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. Giữ entrypoint gốc của Hadoop
CMD ["/entrypoint.sh"]

