Kế hoạch báo cáo distributed system course project

- Kiến trúc Lakehouse  
- Phân tích dữ liệu lớn cho đồng hồ điện/nước dựa trên IoT  
- Pipeline, 2 giai đoạn  
  - Training model  
  - Streaming \+ predict  
- Hệ thống gồm các module  
  - Data collection \+ message queue: Kafka  
    - Gồm có 2 broker tượng trưng, nhận dữ liệu từ 1 file csv cho sẵn và streaming data tới lớp tiếp theo để xử lý  
  - Data analytics: Spark (2 lớp)  
    - filtering, sampling, integration: Nhận data và xử lý data từ lớp trước. Đồng thời lưu dữ liệu vào lớp data stỏage  
    - predict data:  
      - Ở bước training Load model ML (sử dụng model đơn giản nhất, ít phức tạp nhất nhưng adaptive với hệ thống chung) lên để training   
      - Ở bước inference: Dựa vào 3 ngày đầu tiên của 1 tháng, sau đó liên tục dự đoán tổng lượng điện có thể tiêu thụ trong tháng đó cho tới khi đủ số ngày trong tháng đó. Làm tiếp tục với tháng tiếp theo.  
  - Data storage: HDFS: Lưu trữ data streaming và model ML  
  - Visualization: vẽ biểu đồ số điện tiêu thụ trong 1 khoảng tg được lưu ở storage trong 1 tháng, 1 năm và 3 năm

---
### **2. Kế hoạch & Lộ trình Triển khai 🗺️**

Mục tiêu là xây dựng một kiến trúc Lakehouse đơn giản hóa cho dữ liệu Đồng hồ Thông minh IoT. Dưới đây là cách các thành phần bạn đã chọn ánh xạ tới các yêu cầu:

- **Lớp Thu thập (Ingestion Layer - Kafka):** Cần 2 Broker để tạo một hàng đợi phân tán (distributed queue). Trong Docker, bạn cũng sẽ cần một dịch vụ Zookeeper (hoặc sử dụng Kafka ở chế độ KRaft) để quản lý các broker này.

- **Lớp Xử lý (Processing Layer - Spark):**
  - **Xử lý theo lô (Batch - Training):** Một Spark job đọc dữ liệu CSV lịch sử, huấn luyện một mô hình hồi quy hoặc chuỗi thời gian, và lưu mô hình vào HDFS.
  - **Xử lý luồng (Streaming - Inference):** Một Spark Streaming job đăng ký (subscribe) vào Kafka, tải mô hình từ HDFS, và dự đoán mức tiêu thụ hàng tháng dựa trên logic "3 ngày đầu tiên".

- **Lớp Lưu trữ (Storage Layer - HDFS):** Bạn đã chỉ định một "Single Node". Nó sẽ hoạt động như Data Lake của bạn và lưu trữ:
  - Dữ liệu thô (được lưu trữ từ luồng).
  - Tệp mô hình ML đã được huấn luyện.

- **Lớp Trình bày (Presentation Layer - Streamlit):** Bạn đã chọn Streamlit (Python) thay vì Tableau/Kibana. Đây là một lựa chọn thông minh cho một dự án Docker vì Streamlit có thể dễ dàng chạy trong một container và đọc trực tiếp từ HDFS hoặc một volume được chia sẻ.

#### **Quy trình Triển khai (Từng bước)**

Chúng ta sẽ chia quy trình này thành 3 Giai đoạn để phù hợp với kế hoạch "Kiểm thử" của bạn.

**Giai đoạn 1: Hạ tầng (Docker Compose)**

Chúng ta cần một tệp `docker-compose.yml` duy nhất để khởi tạo toàn bộ cụm.
- **Mạng (Network):** Tạo một mạng bridge tùy chỉnh (ví dụ: `smart-meter-net`) để các container có thể giao tiếp với nhau bằng tên (ví dụ: `spark-master` có thể nói chuyện với `kafka-broker-1`).
- **Volumes:**
  - Ánh xạ một thư mục cục bộ `./data` vào container HDFS để lưu trữ dữ liệu lâu dài.
  - Ánh xạ một thư mục cục bộ `./app` vào container Spark để bạn có thể chỉnh sửa các kịch bản Python trên máy và chạy chúng trong Docker ngay lập tức
- Kết quả: 
  - Đã connect được stream data flow: Kafka --> Spark --> HDFS
  - Mount disk `namenode_data` và `datanode_data` vào container
  - Trích xuất parquet lên visualizer
  - 
**Giai đoạn 2: Logic Pipeline (Phát triển)**

**A. Trình tạo Dữ liệu giả (Python Script)**
Thay vì dùng một tệp CSV tĩnh, hãy viết một kịch bản Python để:
- Đọc dữ liệu mẫu để hiểu phân phối dữ liệu.
- Mô phỏng các "tick" dữ liệu trực tiếp.
- Sử dụng thư viện `kafka-python` để đẩy các thông điệp JSON đến các Kafka Broker.

**Kết quả 27/11: DONE**

**B. Xử lý "Lakehouse" (Spark)**
- **Job Huấn luyện (`train.py`):**
  - **Đầu vào:** CSV lịch sử.
  - **Hành động:** Huấn luyện một mô hình Machine Learning (chưa xác định)
  - **Đầu ra:** Lưu mô hình vào `hdfs://namenode:8020/models/consumption_model`.
- **Job Xử lý Luồng (`stream.py`):**
  - **Đầu vào:** Luồng Kafka (`readStream`).
  - **Logic:** Sử dụng hàm cửa sổ (Windowing function). Tích lũy dữ liệu trong 3 ngày (thời gian mô phỏng).
  - **Hành động:** Tải mô hình từ HDFS → Dự đoán Tổng lượng tiêu thụ trong tháng.
  - **Đầu ra:** Ghi kết quả vào HDFS (để Streamlit đọc) hoặc trả lại một topic Kafka khác.

**Giai đoạn 3: Đánh giá & Trực quan hóa**

- **Ứng dụng Streamlit:** Container này cần cài đặt HDFS client (thư viện `hdfs` cho Python) hoặc có quyền truy cập vào volume chia sẻ để đọc kết quả đã xử lý và vẽ biểu đồ so sánh "Dự đoán vs. Thực tế".
- **Kiểm thử Hiệu năng (Stress Test):**
  Để đáp ứng yêu cầu "Kiểm thử Dưới Tải nặng", chúng ta sẽ tham số hóa Trình tạo Dữ liệu giả.
  - **Tải bình thường:** 1 thông điệp/giây.
  - **Tải nặng:** 1000 thông điệp/giây (sử dụng đa luồng cho trình tạo).
---

### **3. Khung Đánh giá & Kiểm thử Hiệu năng ⚙️**

Làm sao để chắc chắn hệ thống sẽ hoạt động tốt trong thực tế? Chúng ta sẽ thực hiện một quy trình kiểm thử hiệu năng gồm 3 bước:

1. **Thiết lập Hiệu năng Cơ sở (Baseline Performance):** Đầu tiên, chúng ta sẽ chạy hệ thống dưới một mức tải giả lập thông thường. Kết quả từ bước này sẽ là "điểm chuẩn" để so sánh và đánh giá tất cả các thay đổi sau này.  
2. **Kiểm thử Dưới Tải nặng (Stress Testing):** Tiếp theo, chúng ta sẽ đẩy hệ thống đến giới hạn của nó bằng cách tăng đột biến lượng dữ liệu đầu vào, giả lập các kịch bản như giờ cao điểm hoặc sự cố trên diện rộng. Mục tiêu là để tìm ra điểm nghẽn (bottleneck) của kiến trúc và xem hệ thống phản ứng ra sao. Liệu độ trễ có tăng vọt? Hàng đợi xử lý có bị ùn ứ không?  
3. **Kiểm thử Khả năng Phục hồi (Resilience Testing):** Cuối cùng, chúng ta sẽ mô phỏng các sự cố thực tế, ví dụ như cho một máy chủ trong cụm bị lỗi. Mục tiêu là để xác minh rằng hệ thống có khả năng tự động phục hồi mà không gây mất mát dữ liệu và quay trở lại hoạt động bình thường trong thời gian ngắn nhất.

