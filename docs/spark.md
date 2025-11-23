# Apache Spark: Lý thuyết nền tnarg

## Mục lục
<!-- TOC -->
  * [Chương 1. Giới thiệu và Bối cảnh Lịch sử: Từ MapReduce đến Kỷ nguyên In-Memory](#chương-1-giới-thiệu-và-bối-cảnh-lịch-sử-từ-mapreduce-đến-kỷ-nguyên-in-memory)
    * [1.1. Sự Thoái trào của Kỷ nguyên Đĩa cứng và Hạn chế của MapReduce](#11-sự-thoái-trào-của-kỷ-nguyên-đĩa-cứng-và-hạn-chế-của-mapreduce)
    * [1.2. Cuộc cách mạng In-Memory Computing của Spark](#12-cuộc-cách-mạng-in-memory-computing-của-spark)
    * [1.3. Ba trụ cột thiết kế cốt lõi](#13-ba-trụ-cột-thiết-kế-cốt-lõi)
  * [Chương 2. Giải phẫu kiến trúc Spark: Cơ chế Master-Slave và hệ sinh thái Cluster](#chương-2-giải-phẫu-kiến-trúc-spark-cơ-chế-master-slave-và-hệ-sinh-thái-cluster)
    * [2.1. The Driver (Master) - Bộ não điều phối](#21-the-driver-master---bộ-não-điều-phối)
    * [2.2. The Executors (Slaves) - Động cơ tính toán](#22-the-executors-slaves---động-cơ-tính-toán)
    * [2.3. CLuster Manager - Người quản lý tài nguyên](#23-cluster-manager---người-quản-lý-tài-nguyên)
    * [2.4. Chế độ triển khai: Client Mode vs. Cluster Mode](#24-chế-độ-triển-khai-client-mode-vs-cluster-mode)
  * [Chương 3. Hệ thống Cấu trúc dữ liệu: Từ RDD đến Dataset](#chương-3-hệ-thống-cấu-trúc-dữ-liệu-từ-rdd-đến-dataset)
    * [3.1. RDD (Resilient Distributed Dataset) - Nguyên tử của Spark](#31-rdd-resilient-distributed-dataset---nguyên-tử-của-spark)
    * [3.2. DataFrame - sự cách mạng về cấu trúc và tối ưu hoá](#32-dataframe---sự-cách-mạng-về-cấu-trúc-và-tối-ưu-hoá)
    * [3.3. Dataset - Sự hợp nhất (Type-Safe + Performance)](#33-dataset---sự-hợp-nhất-type-safe--performance)
  * [Chương 4. Cơ chế thực thi: Lazy Evaluation và sức mạnh của DAG](#chương-4-cơ-chế-thực-thi-lazy-evaluation-và-sức-mạnh-của-dag)
    * [4.1. Lazy Evaluation: Chiến lược "Chậm mà chắc"](#41-lazy-evaluation-chiến-lược-chậm-mà-chắc)
    * [4.2. DAG (Directed Acyclic Graph) và phân rã Stages](#42-dag-directed-acyclic-graph-và-phân-rã-stages)
  * [Chương 5. The Shuffle: Nút thắt cổ chai về hiệu năng](#chương-5-the-shuffle-nút-thắt-cổ-chai-về-hiệu-năng)
    * [5.1. Cơ chế vật lý của Shuffle: Map-Side và Reduce-Side](#51-cơ-chế-vật-lý-của-shuffle-map-side-và-reduce-side)
    * [5.2. Sự tiến hoá của Shuffle Managers: Hash vs. Sort](#52-sự-tiến-hoá-của-shuffle-managers-hash-vs-sort)
    * [5.3. Tác động của Shuffle và Cấu hình](#53-tác-động-của-shuffle-và-cấu-hình)
  * [Chương 6. Quản lý bộ nhớ (Memory Management): Unified Memory Manager](#chương-6-quản-lý-bộ-nhớ-memory-management-unified-memory-manager)
    * [6.1. Cấu trúc Heap của Executor](#61-cấu-trúc-heap-của-executor)
    * [6.2. Cơ chế chiếm dụng động (Dynamic Occupancy)](#62-cơ-chế-chiếm-dụng-động-dynamic-occupancy)
    * [6.3. Off-Heap Memory](#63-off-heap-memory)
  * [Chương 7. Tối ưu hoá truy vấn: Catalyst Optimizer và Tungsten Engine](#chương-7-tối-ưu-hoá-truy-vấn-catalyst-optimizer-và-tungsten-engine)
    * [7.1. Catalyst Optimizer: Bộ não chiến lược](#71-catalyst-optimizer-bộ-não-chiến-lược)
    * [7.2. Tungsten Execution Engine: Tối ưu phần cứng](#72-tungsten-execution-engine-tối-ưu-phần-cứng)
    * [7.3. Adaptive Query Execution (AQE)](#73-adaptive-query-execution-aqe)
  * [Chương 8. Chiến lược phân vùng (Partition Strategy)](#chương-8-chiến-lược-phân-vùng-partition-strategy)
    * [8.1. Nghệ thuật chọn kích thước Partition](#81-nghệ-thuật-chọn-kích-thước-partition)
    * [8.2. Repartition vs. Coalesce](#82-repartition-vs-coalesce)
    * [8.3. Vấn đề Data Skew (Lệch dữ liệu)](#83-vấn-đề-data-skew-lệch-dữ-liệu)
  * [Chương 9. Chuyển đổi tư duy: Từ Pandas sang Spark](#chương-9-chuyển-đổi-tư-duy-từ-pandas-sang-spark)
    * [9.1. Sự khác biệt cốt lõi về mô hình tư duy](#91-sự-khác-biệt-cốt-lõi-về-mô-hình-tư-duy)
    * [9.2. Khi nào nên dùng Spark thay vì Pandas?](#92-khi-nào-nên-dùng-spark-thay-vì-pandas-)
    * [Cầu nối: Pandas API on Spark](#cầu-nối-pandas-api-on-spark)
  * [Kết luận](#kết-luận-)
<!-- TOC -->


## Chương 1. Giới thiệu và Bối cảnh Lịch sử: Từ MapReduce đến Kỷ nguyên In-Memory
### 1.1. Sự Thoái trào của Kỷ nguyên Đĩa cứng và Hạn chế của MapReduce

Để thấu hiểu lý do Apache Spark trở thành tiêu chuẩn vàng trong xử lý dữ liệu lớn hiện đại,
chúng ta bắt buộc phải xem xét bối cảnh lịch sử mà nó ra đời. Trước khi Spark xuất hiện, hệ 
sinh thái Big Data bị thống trị bởi Hadoop MapReduce. Mặc dù MapReduce đã giải quyết được bài 
toán phân tán tính toán trên quy mô lớn, kiến trúc của nó tồn tại một nhược điểm chí mạng về hiệu
năng: sự phụ thuộc quá mức vào I/O đĩa cứng (Disk I/O). 

Trong mô hình MapReduce truyền thống, quy trình xử lý dữ liệu là một chuỗi tuần tự cứng nhắc: 
Đọc từ đĩa $\rightarrow$ Map $\rightarrow$ Ghi xuống đĩa $\rightarrow$ Shuffle $\rightarrow$ Reduce
$\rightarrow$ Ghi kết quả cuối cùng xuống đĩa. Đặc biệt, giữa mỗi giai đoạn Map và Reduce, dữ liệu trung gian
bắt buộc phải được ghi xuống hệ thống tệp phân tán (HDFS) để đảm bảo khả năng chịu lỗi. Cơ chế này tạo ra một 
độ trễ (latency) khổng lồ, đặc biệt là đối với các thuật toán lặp (iterative algorithms) phổ biến trong Machine 
Learning như K-means hay Logistic Regression, nơ dữ liệu cần được tái sử dụng hàng trăm lần. Mỗi vòng lặp MapReduce
đồng nghĩa với việc đọc và ghi lại toàn bộ tập dữ liệu xuống ổ cứng, làm lãng phí tài nguyên CPU và băng thông đĩa. 

### 1.2. Cuộc cách mạng In-Memory Computing của Spark

Apache Spark ra đời như một sự tiến hoá vượt bậc, giải quyết vấn đề I/O của MapReduce bằng triết lý **In-Memory Computing**
(Tính toán trong bộ nhớ). Thay vì ghi dữ liệu trung gian xuống ổ cứng sau mỗi bước biến đổi, Spark giữ dữ liệu (dưới dạng RDD hoặc DataFrame)
trong bộ nhớ RAM của các máy chủ trong cụm (cluster). 

Sự thay đổi kiến trúc này cho phép Spark đạt được tốc độ xử lý nhanh hơn từ 10 đến 100 lần so với MapReduce đối với các tác vụ nhất định,
đặc biệt là các tác vụ dòi hỏi truy cập dữ liệu lặp đi lặp lại. Tuy nhiên, cần lưu ý rằng Spark không loại bỏ hoàn toàn việc ghi dữ liệu. 
Nó sử dụng bộ nhớ một cách thông minh và chỉ ghi xuống đĩa (spill to disk) khi bộ nhớ không đủ chứa hoặc tỏng giai đoạn shuffle phức tạp 
để đảm bảo tính ổn định. 

### 1.3. Ba trụ cột thiết kế cốt lõi
Hệ thống Spark được xây dựng dựa trên ba nguyên tắc thiết kế bất biến mà mọi kỹ sư dữ liệu cần nắm vững: 
1. **Tính toán phân tán (Distributed Processing):** Spark chia nhỏ một công việc khổng lồ thành hàng nghìn tác vụ (tasks) nhỏ
và phân phối chúng để xử lý song song trên nhiều máy tính vật lý. Điều này cho phép hệ thống mở rộng quy mô (scale-out)
một cách tuyến tính khi dữ liệu tăng lên.
2. **Đánh giá lười (Lazy Evaluation):** Khác với công cụ xử lý truyền thống thực thi ngay lập tức, Spark chờ đợi. Khi người dùng định nghĩa 
một chuỗi các thao tác, Spark không chạy ngay mà ghi nhớ chúng lại. Chỉ khi có một hành động (Action) yêu cầu kết quả thực tế, Spark mới xây 
dựng một kế hoạch thực thi tối ưu (DAG) và chạy toàn bộ quy trình một lần. Điều này cho phép hệ thống tối ưu hoá toàn cục, tránh các bước tính
toán dư thừa. 
3. **Khả năng chịu lỗi (Fault Tolerance) thông qua Lineage:** Trong một hệ thống với hàng nghìn máy chủ, hư hỏng phần cứng là điều không thể 
tránh khỏi. Thay vì sao chép dữ liệu liên tục để dự phòng (như HDFS), Spark ghi nhớ "công thức" (lineage) tạo ra dữ liệu. Nếu một phần dữ liệu
bị mất do máy hỏng, Spark chỉ cần chạy lại các bước biến đổi trên phần dữ liệu đó để tái tạo lại, giúp tiết kiệm không gian lưu trữ và băng thông. 


## Chương 2. Giải phẫu kiến trúc Spark: Cơ chế Master-Slave và hệ sinh thái Cluster
Kiến trúc của Spark tuân thủ nghiêm ngặt mô hình **Master-Slave** (chủ-tớ). Để vận hành một ứng dụng Spark hiệu quả, việc hiểu rõ vai trò, 
trách nhiệm và giới hạn từng thành phần trong kiến trúc này là tối quan trọng.

### 2.1. The Driver (Master) - Bộ não điều phối

Driver là tiến trình quan trọng nhất, đóng vai trò là "trung tâm thần kinh" của một ứng dụng Spark. 
Đây là nơi phương thức `main()` của chương trình người dùng thực thi và là nơi khởi tạo `SparkContext`
(hoặc `SparkSession` trong các phiên bản mới hơn). 

**Chức năng chi tiết của Driver:**
* **Biên dịch Code thành Task:** Driver chịu trách nhiệm phân tích code của người dùng, chuyển đổi các phép biến đổi logic (transformation)
thành một đồ thị các tác vụ vật lý (DAG - Directed Acycle Graph). 
* **Lập lịch (Scheduling):** Driver quyết định thứ tự thực thi của các tác vụ, xác định tác vụ nào có thể chạy song song và tác vụ nào phải chờ đợi. 
* **Quản lý Metadata:** Driver nắm giữ thông tin toàn cục về trạng thái của ứng dụng, bao gồm vị trí của các khối dữ liệu (block locations), 
trạng thái của Executors, và tiến độ của các Job. 

**Cảnh báo về kiến trúc:** Driver là một điểm nghẽn tiềm tàng (single point of failure) và là nơi dễ xảy ra lỗi tràn bộ nhớ (OOM) nhất. Vì Driver thường
là một tiến trình Java đơn lẻ, nếu người dùng cố gắng kéo (collect) quá nhiều dữ liệu về Driver hoặc thực hiện các thao tác broadcast quá lớn, Driver sẽ bị
quả tải và làm sập toàn bộ ứng dụng. 

### 2.2. The Executors (Slaves) - Động cơ tính toán
Nếu Driver là kiến trúc sư, thì Executors là những công nhân xây dựng trực tiếp thực hiện công việc. Executors là các tiến trình JVM (Java Virtual Machine) 
chạy trên các nút làm việc (Worker Nodes) của cụm. 

**Nhiệm vụ cốt lõi của Executor:**
* **Thực thi Task:** Executor nhận các đoạn code (task) đã được serialized từ Driver và chạy chúng trên dữ liệu cục bộ mà nó quản lý. 
Mỗi Executor có thể chạy nhiều task song song bằng cách sử dụng nhiều luồng (threads).
* **Lưu trữ dữ liệu (Caching & Storage):** Đây là điểm khác biệt chính so với các hệ thống khác. Executor cung cấp bộ nhớ (RAM) để lưu trữ
các RDD hoặc DataFrame được cache lại. Dữ liệu tỏng Spark không nằm tập trung ở Driver mà nằm rải rac trên RAM của các Executors.
* **Giao tiếp:** Executor liên tục gửi nhịp tim (heartbeat) và trạng thái thực thi của Task cho Driver. Nếu Driver không nhận được tín hiệu này,
nó sẽ coi Executor đã chết và kích hoạt cơ chế chịu lỗi. 

### 2.3. CLuster Manager - Người quản lý tài nguyên

Một hiểu lầm phổ biến là Spark tự quản lý các máy chủ vật lý. Thực tế, Spark là một động cơ tính toán (Compute Engine), không phải là một hệ thống quản
lý tài nguyên (Resource Manager). Spark phải nhờ cậy vào một bên thứ 3 gọi là Cluster Manager để xin cấp phát CPU và RAM. 

**Bảng so sánh các loại Cluster Manager:**

<table>
  <caption>Bảng so sánh các loại Cluster Manager:</caption>
  <thead>
    <tr>
      <th>Cluster Manager</th>
      <th>Đặc điểm</th>
      <th>Ưu điểm</th>
      <th>Nhược điểm</th>
      <th>Trường hợp sử dụng</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Standalone</td>
      <td>Tích hợp sẵn trong Spark</td>
      <td>Dễ triển khai, nhẹ, khởi động nhanh</td>
      <td>Ít tính năng quản lý, khó chia sẻ tài nguyên với app khác</td>
      <td>Phát triển, Test, Cluster nhỏ</td>
    </tr>
    <tr>
      <td>Hadoop YARN</td>
      <td>Chuẩn công nghiệp</td>
      <td>Tích hợp sâu với HDFS, bảo mật Kerberos, quản lý đa tenant</td>
      <td>Cấu hình phức tạp, nặng nề hơn</td>
      <td>Hệ thống Big Data doanh nghiệp</td>
    </tr>
    <tr>
      <td>Kubernetes (K8s)</td>
      <td>Xu hướng hiện đại</td>
      <td>Cô lập tài nguyên tốt (Container), linh hoạt, Cloud-native</td>
      <td>Phức tạp trong cấu hình mạng và lưu trữ</td>
      <td>Môi trường Cloud, Microservices</td>
    </tr>
    <tr>
      <td>Mesos</td>
      <td>Quản lý tổng quát</td>
      <td>Khả năng mở rộng cao</td>
      <td>Cộng đồng hỗ trợ đang giảm dần</td>
      <td>Các hệ thống cũ hoặc đặc thù</td>
    </tr>
  </tbody>
</table>

### 2.4. Chế độ triển khai: Client Mode vs. Cluster Mode
Việc hiểu sự khác biệt giữa hai chế độ này là cực kỳ quan trọng cho việc đưa ứng dụng lên môi trường sản xuất (Production).
* **Client Mode:** Driver chạy trên máy chủ mà bạn gõ lệnh `spark-submit` (ví dụ: máy laptop cá nhân hoặc Edge Node).
  * **Ưu điểm:** Dễ Debug, xem log trực tiếp trên màn hình console. 
  * **Nhược điểm:** Nếu máy laptop bị tắt hoặc mất mạng, ứng dụng chết. Driver tranh chấp tài nguyên mạng với máy cá nhân. 

* **Cluster Mode:** Driver được đẩy lên chạy trên một trong các Worker Node bên trong cụm (do Cluster Manager chỉ định).
  * **Ưu điểm:** Ổn định, Driver nằm gần Executors giúp giảm độ trễ mạng, máy client có thể ngắt kết nối mà không ảnh hưởng đến ứng dụng. 
  * **Nhược điểm:** Khó xem log hơn (phải truy cập qua YARN / Spark UI).

## Chương 3. Hệ thống Cấu trúc dữ liệu: Từ RDD đến Dataset

Sự phát triển của các API dữ liệu trong Spark phản ánh quá trình chuyển dịch từ việc kiểm soát cấp thấp (Low-level Control) sang tối ưu hoá tự động. 

### 3.1. RDD (Resilient Distributed Dataset) - Nguyên tử của Spark
RDD là cấu trúc dữ liệu nền tảng nhất, xuất hiện từ phiên bản Spark 1.0. Mọi cấu trúc cấp cao (DataFrame, Dataset) cuối cùng đều được biên dịch thành các thao tác RDD.

* **Bản chất:** RDD là một tập hợp các đối tượng Java/Scala/Python phân tán, bất biến. Nó cung cấp các API chức năng như `map`, `reduce`, `filter`.
* **Hạn chế "Hộp đen":** RDD hoạt động với các đối tượng mờ đục (opaque objects). Khi bạn viết `rdd.map(lambda x: x * 2)`, Spark không hề biết biến `x` là gì hay phép toán 
`* 2` thực hiện như thế nào. Spark chỉ biết serialize đối tượng, gửi đến Executor , và deserialize để chạy hàm. Điều này ngăn cản Spark thực hiện các tối ưu hoá sâu bên trong
  (ví dụ: sắp xếp lại các lịch). 

### 3.2. DataFrame - sự cách mạng về cấu trúc và tối ưu hoá
DataFrame (ra mắt trong Spark 1.3) mang đến khái niệm **Schema** (lược đồ dữ liệu) cho Spark. Dữ liệu không còn là các đối tượng vô danh mà được tổ chức thành 
các cột có tên và kiểu dữ liệu rõ ràng (như bảng trong SQL).

Sự vượt trội của DataFrame so với RDD nằm ở **Catalysit Optimizer:**
* Vì Spark hiểu cấu trúc dữ liệu (biết cột nào là `Integer`, cột nào là `String`) và hiểu ý định của người dùng (ví dụ: lọc cột `Age > 20`), nó có thể can thiệp để tối ưu hoá. 
* **Ví dụ:** Nếu bạn dùng RDD, bạn phải đọc toàn bộ dữ liệu rồi mới lọc. Với DataFrame, Spark sử dụng kỹ thuật Predicate Pushdown, 
để đẩy điều kiện lọc xuống tầng lưu trữ (như Parquet hoặc JDBC), chỉ đọc những bản ghi thoả mãn điều kiện, giúp giảm I/O đáng kể. 

### 3.3. Dataset - Sự hợp nhất (Type-Safe + Performance)
Dataset (Spark 1.6+) là nỗ lực kết hợp an toàn về kiểu dữ liệu (Type Safety) của RDD và hiệu năng của DataFrame.
* **Type Safety:** Trong DataFrame, nếu bạn gõ sai tên cột `df.select("name")`, lỗi chỉ xuất hiện khi chạy chương trình (Runtime). Với Dataset (trong Java/Scala), 
lỗi này sẽ bị bắt ngay khi biên dịch (Compile-time), giúp giảm thiểu lỗi trong production. 
* **Tunsten Encoders:** Dataset sử dụng Encoders để chuyển đổi trực tiếp các đối tượng JVM thành định dạng nhị phân nội bộ của Spark (Tungsten binary format). 
Điều này giúp thao tác trên dữ liệu nhanh hơn và tốn ít bộ nhớ hơn so với việc dùng Java Serialization của RDD. 
* **Lưu ý:** Trong Python (PySpark), không có API Dataset riêng biệt do tính chất động của ngôn ngữ. DataFrame trong PySpark thực chất là Dataset.

**Bảng so sánh chi tiết RDD, DataFrame và Dataset:**

<table>
  <caption>Bảng so sánh chi tiết RDD, DataFrame và Dataset:</caption>
  <thead>
    <tr>
      <th>Đặc điểm</th>
      <th>RDD</th>
      <th>DataFrame</th>
      <th>Dataset</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Ra đời</td>
      <td>Spark 1.0</td>
      <td>Spark 1.3</td>
      <td>Spark 1.6</td>
    </tr>
    <tr>
      <td>Cấu trúc dữ liệu</td>
      <td>Không cấu trúc (Unstructured objects)</td>
      <td>Có cấu trúc (Structured rows)</td>
      <td>Có cấu trúc & định kiểu (Typed objects)</td>
    </tr>
    <tr>
      <td>Tối ưu hóa</td>
      <td>Không (Người dùng tự tối ưu)</td>
      <td>Có (Catalyst Optimizer)</td>
      <td>Có (Catalyst Optimizer + Tungsten)</td>
    </tr>
    <tr>
      <td>Type Safety</td>
      <td>Compile-time</td>
      <td>Runtime</td>
      <td>Compile-time</td>
    </tr>
    <tr>
      <td>Ngôn ngữ</td>
      <td>Java, Scala, Python, R</td>
      <td>Java, Scala, Python, R</td>
      <td>Java, Scala (Python dùng DF)</td>
    </tr>
    <tr>
      <td>Serialization</td>
      <td>Java/Kryo (Chậm, tốn bộ nhớ)</td>
      <td>Tungsten Binary (Nhanh, gọn)</td>
      <td>Tungsten Encoders (Nhanh, gọn)</td>
    </tr>
    <tr>
      <td>Trường hợp dùng</td>
      <td>Dữ liệu phi cấu trúc, thuật toán phức tạp cấp thấp</td>
      <td>ETL, SQL, Phân tích dữ liệu có cấu trúc</td>
      <td>Phát triển ứng dụng lớn cần an toàn kiểu</td>
    </tr>
  </tbody>
</table>

## Chương 4. Cơ chế thực thi: Lazy Evaluation và sức mạnh của DAG

Hiểu cách Spark chuyển đổi code thành hành động thực tế là chìa khoá để viết code hiệu năng cao. 
Mô hình thực thi của Spark khác biệt hoàn toàn so với lập trình tuần tự truyền thống.

### 4.1. Lazy Evaluation: Chiến lược "Chậm mà chắc"

Trong Spark, các thao tác được chia thành hai loại: **Transformations** (Biến đổi) và **Actions** (Hành động).

* **Transformations (Lazy):** Bao gồm các hàm như `map`, `filter`, `join`, `groupBy`. Khi bạn gọi các hàm này, Spark
**không xử lý dữ liệu ngay lập tức.** Thay vào đó, nó chỉ ghi lại "ý định" của bạn vào một kế hoạch logic.
* **Actions (Eager):** Bao gồm các hàm như `count`, `collect`, `save`, `show`. Chỉ khi một Action được gọi, 
Spark mới thực sự kích hoạt quá trình tính toán toàn bộ chuỗi biến đổi trước đó. 

**Tại sao Lazy Evaluation lại quan trọng?** Sự "lười biếng" này cho phép Spark tối ưu hoá toàn cục. Giả sử bạn viết code 
để đọc một file 100GB, sau đó sắp xếp (`sort`), rồi lọc (`filter`), và cuối cùng chỉ lấy 10 dòng đầu tiên (`take(10)`).
* Nếu thực thi ngay lập tức (Eager): Hệ thống sẽ đọc hết 100GB, sắp xếp hết 100GB (tốn rất nhiều RAM/Disk), lọc hết, 
rồi mới lấy 10 dòng. Rất lãng phí. 
* Với Lazy Evaluation: Spark nhìn thấy toàn bộ kế hoạch và nhận ra bạn chỉ cần 10 dòng. Nó sẽ thay đổi chiến lược: 
đọc và lọc dữ liệu trước, chỉ sắp xếp một phần nhỏ cần thiết, và dừng ngay khi tìm đủ 10 dòng. 
Điều này tiết kiệm tài nguyên khổng lồ. 

### 4.2. DAG (Directed Acyclic Graph) và phân rã Stages

Khi một Action được kích hoạt, Driver sẽ chuyển đổi chuỗi các Transformations thành một **DAG (Đồ thị có hướng không chu trình).**
DAG Scheduler sau đó sẽ chịu trách nhiệm phân rã đồ thị này thành các **Stages** (Giai đoạn) nhỏ hơn để thực thi. 

Ranh giới giữa các Stages được xác định dựa trên loại phụ thuộc (Dependency) giữa các RDD:
* **Narrow Dependency (Phụ thuộc hẹp):** Xảy ra khi mỗi partition của RDD cha chỉ được sử dụng bởi tối đa một partition của RDD con. 
  * Ví dụ: `map`, `filter`, `union`, `coalesce`
  * Cơ chế: Các thao tác này có thể được thực hiện liên tiếp trên cùng một node mà không cần di chuyển dữ liệu qua mạng. 
Spark sẽ gom nhóm (pipeline) chúng lại thành một Stage duy nhất để chạy cực nhanh. 
* **Wide Dependency (Phụ thuộc rộng):** Xảy ra khi mỗi partition của RDD con phụ thuộc vào dữ liệu từ nhiều partition của RDD cha. 
  * Ví dụ: `groupByKey`, `reduceByKey`, `join`, `repartition`
  * Cơ chế: Để tính toán kết quả, dữ liệu bắt buộc phải được tráo đổi (shuffle) giữa các Executor trên toàn mạng lưới. Đây là ranh giới
cho đến khi Stage trước hoàn tất việc ghi dữ liệu shuffle. 

**Minh họa về Stages:** Nếu bạn có câu lệnh: `df.read.filter(...).map(...).groupBy(...).count().show()`
1. Stage 1: `read` $\rightarrow$ `filter` $\rightarrow$ `map` (Tất cả là Narrow Dependency, chạy chung trong 1 pipeline).
2. Shuffle Boundary (do `groupBy`).
3. Stage 2: `count` $\rightarrow$ `show` (Sau khi dữ liệu đã được shuffle sang các node mới).

## Chương 5. The Shuffle: Nút thắt cổ chai về hiệu năng

Shuffle là thao tác phức tạp, tốn kém và dễ gỡ lỗi nhất trong Spark. Nó là quá trình tái phân phối
(redistribute) dữ liệu giữa các Executors để nhóm các dữ liệu liên quan với nhau 
(ví dụ: đưa tất cả các bản ghi có cùng key về một máy).

### 5.1. Cơ chế vật lý của Shuffle: Map-Side và Reduce-Side

Quá trình Shuffle không chỉ đơn thuần là truyền dữ liệu qua mạng. Nó bao gồm các bước nặng nhọc về Disk I/O để đảm bảo chịu lỗi. 

1. **Shuffle Write (Map Side):** Tại giai đoạn trước shuffle, các Executor tính toán dữ liệu và xác định xem mỗi bản ghi sẽ thuộc 
về partition nào tiếp theo. Sau đó, thay vì gửi ngay qua mạng, chúng **ghi dữ liệu xuống ở cứng cục bộ (local disk)** của chính mình, 
chia thành các file tương ứng. Việc ghi đĩa này đảm bảo rằng nếu quá trình truyền qua mạng bị lỗi hoặc Executor nhận bị chết, dữ liệu gốc
vấn còn đó để gửi lại mà không cần tính toán lại từ đầu. 
2. **Shuffle Read (Reduce Side):** Các Executor ở giai đoạn sau (Reducers) sẽ gửi yêu cầu đến các Executor map-side để kéo (pull) các khối
dữ liệu thuộc về mình. Dữ liệu được truyền qua mạng, giải nén (deserialize) và nạp vào bộ để thực hiện các phép gộp (aggregation) hoặc join. 

### 5.2. Sự tiến hoá của Shuffle Managers: Hash vs. Sort

Spark đã trải qua nhiều lần cải tiến cơ chế Shuffle để tối ưu hiệu năng:
* **Hash Shuffle (Cũ):** Dữ liệu không được sắp xếp mà chỉ được băm (hash) vào các file. Nhược điểm lớn là nó tạo ra số lượng file trung gian khổng lồ
(bằng số Mappers x số Reducers), gây quá tải cho hệ điều hành và tốn bộ nhớ đệm. 
* **Sort Shuffle (tiêu chuẩn hiện tại):** Dữ liệu được sắp xếp (sort) trước khi ghi ra đĩa. Một Executor chỉ tạo ra một file dữ liệu lớn và một file index
cho mỗi map task, tahy vì hàng ngàn file nhỏ. Điều này giảm thiểu ngẫu nhiên I/O và tiết kiệm bộ nhớ RAM đáng kể. 
* **Bypass Merge Sort Shuffle:** Trong một số trường hợp số lượng partition giảm (reduce partions) nhỏ (mặc định < 200) và không cần gộp dữ liệu (map-side combines), 
Spark có thể quay lại cớ chế giống **Hash Shuffle** để tránh chi phí sắp xếp tốn kém. Đây là lý do tại sao tham số `spark.sql.shuffle.partitions` ảnh hướng lớn đến hiệu năng.  

### 5.3. Tác động của Shuffle và Cấu hình

Shuffle tiêu tốn cả ba loại tài nguyên chính của cụm:
* **Disk I/O:** Ghi và đọc file tạm thời khổng lồ. 
* **Network I/O:** Truyền tải lượng dữ liệu giữa các node. 
* **CPU:** Tiêu tốn cho việc serialization, deserialization, nén, và sắp xếp dữ liệu. 

**Tham số vàng:** `spark.sql.shuffle.partitions` - Giá trị mặc định của tham số này là **200**.
  * **Đối với dữ liệ nhỏ:** 200 là quá lớn, tạo ra nhiều task nhỏ, overhead quản lý cao. 
  * **Đối với dữ liệu lớn (TB):** 200 là quá nhỏ, khiến mỗi partition chứa vài GB dữ liệu, dẫn đến tràn bộ nhớ (OOM) và làm chậm quá trình sort. 


## Chương 6. Quản lý bộ nhớ (Memory Management): Unified Memory Manager

Khả năng quản lý bộ nhớ hiệu quả là yếu tố then chốt để tránh lỗi `OutOfMemoryError` (OOM). Từ phiên bản 1.6, Spark chuyển sang sử dụng mô hình **Unified Memory Manager**,
cho phép linh hoạt chia sẻ bộ nhớ giữa lưu trữ và tính toán. 

### 6.1. Cấu trúc Heap của Executor

Bộ nhớ RAM (JVM Heap) của một Executor được chia thành ba vùng chính một cách logic.
**Reserved Memory (Bộ nhớ dự trữ):** Một lượng cố định (khoảng 300 MB) được dành riêng cho các hoạt động nội tại của engine Spark. Người dùng không thể can thiệp vào vùng này.
**User Memory (Bộ nhớ người dùng):** Mặc định chiếm **40%** (còn lại sau khi trừ Reserved). Vùng này dùng để lưu trữ cấu trúc dữ liệu do người dùng tạo ra trong code (ví dụ: các map, array trong UDF), metadata của RDD). 
Spark không quản lý vùng này, người dùng phải tự chịu trách nhiệm tránh tràn bộ nhớ.
**Spark Memory (Bộ nhớ Spark - Unified):** Mặc định chiếm **60%**. Đây là vùng được Spark quản lý chủ động cho hai mục đích:
* **Storage Memory:** Dùng để lưu trữ dữ liệu Cache (`persist()`, `cache()`) và Broadcast variables. 
* **Execution Memory:** Dùng làm bộ đệm cho phép tính toán tức thời như Shuffle, Join, Sort, Aggregation.

### 6.2. Cơ chế chiếm dụng động (Dynamic Occupancy)

Sự thống nhất (Unified) thực hiện ở ranh giới mềm dẻo giữa Storage và Execution:
* Khi Execution cần thêm bộ nhớ và Storage đang rảnh, nó có thể mượn phần rảnh đó. 
* Ngược lại, Storage cũng có thể mượn của Execution.
* **Quy tắc "Execution thắng thế":** Nếu Execution cần đòi lại bộ nhớ mà Storage đã mượn, Spark sẽ ép Storage phải nhả ra 
(evict) bằng cách xóa dữ liệu cache khỏi RAM hoặc đẩy xuống đĩa. Tuy nhiên, Storage không thể đuổi Execution ra khỏi bộ nhớ. 
Điều này đảm bảo rằng tiến trình tính toán (ví dụ: một lệnh Join đang chạy dở) không bao giờ bị crash chỉ vì người dùng cache quá nhiều dữ liệu.

### 6.3. Off-Heap Memory
Ngoài bộ nhớ Heap (được quản lý bởi Java Garbage Collector - GC), 
Spark hỗ trợ sử dụng bộ nhớ **Off-Heap** (nằm ngoài JVM, quản lý trực tiếp bởi hệ điều hành) thông qua cấu hình `spark.memory.offHeap.enabled`.
* **Lợi ích:** Dữ liệu lưu trên Off-heap không bị GC quét qua, giúp giảm thời gian dừng hệ thống (GC Pauses) khi làm việc với heap lớn (trên 32GB).
* **Ứng dụng:** Thường được sử dụng kết hợp với Project Tungsten để tối ưu hoá việc lưu trữ dữ liệu dạng nhị phân.  

## Chương 7. Tối ưu hoá truy vấn: Catalyst Optimizer và Tungsten Engine

Tại sao Spark SQL/DataFrame lại có thể nhanh hơn RDD viết tay bằng code Java/Scala thuần túy? Câu trả lời nằm ở hai "động cơ" tối ưu hóa mạnh mẽ bên dưới: Catalyst và Tungsten.

### 7.1. Catalyst Optimizer: Bộ não chiến lược

Catalyst là một framework tối ưu hóa truy vấn có thể mở rộng. Khi bạn viết một câu lệnh DataFrame phức tạp, Catalyst sẽ biến nó thành kế hoạch thực thi hiệu quả nhất thông qua 4 giai đoạn:
1. **Analysis (Phân tích):** Spark kiểm tra xem các tham chiếu bảng, tên cột có hợp lệ không bằng cách tra cứu trong Catalog. Nó giải quyết các thuộc tính chưa được phân giải (unresolved attributes).
2. **Logical Optimization (Tối ưu Logic):** Áp dụng hàng loạt quy tắc tối ưu hóa tiêu chuẩn, bất kể dữ liệu vật lý nằm ở đâu:
   * **Predicate Pushdown:** Đẩy các điều kiện lọc (filter) xuống sớm nhất có thể trong cây thực thi, giúp loại bỏ dữ liệu thừa trước khi thực hiện các phép Join đắt đỏ.

   * **Column Pruning:** Loại bỏ các cột không được select ở kết quả cuối cùng để giảm lượng dữ liệu đọc từ đĩa.

   * **Constant Folding:** Tính toán trước các biểu thức hằng số (ví dụ: biến 1 + 2 thành 3 ngay trong plan).   
3. **Physical Planning (Lập kế hoạch vật lý):** ừ một Logical Plan tối ưu, Spark sinh ra nhiều Physical Plans ứng viên. Ví dụ: Để thực hiện một phép Join, 
Spark cân nhắc giữa `SortMergeJoin`(tốt cho bảng lớn) và `BroadcastHashJoin` (tốt cho bảng nhỏ). Nó sử dụng mô hình chi phí (Cost Model) để chọn kế hoạch rẻ nhất.
4. **Code Generation (Sinh mã):** Chuyển đổi Physical Plan cuối cùng thành Java Bytecode để chạy trên các Executor.

### 7.2. Tungsten Execution Engine: Tối ưu phần cứng

Tunsten tập trung vào việc khai thác tối đa hiệu năng phần cứng (CPU và Memory) thay vì tối ưu I/O.

* **Whole-Stage Code Generation:** Trong mô hình truyền thống (Volcano model), mỗi hàng dữ liệu đi qua một chuỗi các hàm gọi (function calls) riêng lẻ cho từng operator (filter, map). 
Điều này gây ra overhead lớn do các lời gọi hàm ảo (virtual function calls). Tungsten gộp toàn bộ logic của một Stage thành một hàm Java duy nhất, hoạt động giống như code được viết tay tối ưu, giúp dữ liệu nằm gọn trong CPU cache L1/L2.

* **Explicit Memory Management:** Tungsten bỏ qua overhead của đối tượng Java (Java Objects thường tốn bộ nhớ gấp 4-5 lần dữ liệu thực). 
Nó quản lý dữ liệu dưới dạng các mảng byte nhị phân (binary data) và sử dụng các con trỏ (pointers) giống như C++, giúp tăng mật độ lưu trữ và giảm áp lực cho GC.

### 7.3. Adaptive Query Execution (AQE)

Từ Spark 3.0, AQE mang đến khả năng tối ưu hóa động. Thay vì chốt cứng kế hoạch thực thi ngay từ đầu, AQE cho phép Spark điều chỉnh kế hoạch ngay trong khi đang chạy dựa trên thống kê thực tế của các Stage đã hoàn thành.   

* **Dynamically Coalescing Shuffle Partitions:** Tự động gộp các partition nhỏ lại để tránh vấn đề file nhỏ sau khi shuffle.

* **Dynamically Switching Join Strategies:** Tự động chuyển từ Sort Merge Join sang Broadcast Join nếu phát hiện một bảng kích thước thực tế nhỏ hơn ngưỡng broadcast.

* **Optimizing Skew Joins:** Tự động phát hiện và chia nhỏ các partition bị lệch dữ liệu (skewed) để cân bằng tải.1

## Chương 8. Chiến lược phân vùng (Partition Strategy)

**Phân vùng (Partitioning)** là đơn vị song song cơ bản của Spark. Hiệu năng của Spark phụ thuộc trực tiếp vào việc bạn chia dữ liệu khéo léo như thế nào.



### 8.1. Nghệ thuật chọn kích thước Partition

### 8.2. Repartition vs. Coalesce

### 8.3. Vấn đề Data Skew (Lệch dữ liệu)

## Chương 9. Chuyển đổi tư duy: Từ Pandas sang Spark

### 9.1. Sự khác biệt cốt lõi về mô hình tư duy

### 9.2. Khi nào nên dùng Spark thay vì Pandas? 


### Cầu nối: Pandas API on Spark

## Kết luận 





