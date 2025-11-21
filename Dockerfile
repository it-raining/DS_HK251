# Sửa dòng này: Dùng bản 'bullseye' để ổn định gói cài đặt
FROM python:3.9-slim-bullseye

# Cài đặt Java 17 (Có sẵn trong kho của Bullseye)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean;

# Thiết lập biến môi trường JAVA_HOME (Đường dẫn chuẩn trên Debian 11/Bullseye)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Tạo thư mục làm việc
WORKDIR /app

# Copy file requirements và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Mặc định giữ container chạy
CMD ["tail", "-f", "/dev/null"]