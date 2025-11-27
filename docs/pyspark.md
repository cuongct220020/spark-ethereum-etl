# Pyspark: Dataframe API, Structred Streaming


## Mục lục
<!-- TOC -->
  * [1. Sự chuyển dịch kiến trúc: Từ Bt](#1-sự-chuyển-dịch-kiến-trúc-từ-bt)
    * [1.1. Sự tiến hoá của hệ sinh thái Spark: DStreams so với Structured Streaming](#11-sự-tiến-hoá-của-hệ-sinh-thái-spark-dstreams-so-với-structured-streaming)
  * [2. Nền tảng lý thuyết: Pyspark DataFrame API và cơ chế thực thi](#2-nền-tảng-lý-thuyết-pyspark-dataframe-api-và-cơ-chế-thực-thi)
    * [2.1. Lazy Evaluation: Chiến lược tối ưu hoá và hiệu năng](#21-lazy-evaluation-chiến-lược-tối-ưu-hoá-và-hiệu-năng)
      * [2.1.1. Phân loại Transformations: Narrow và Wide](#211-phân-loại-transformations-narrow-và-wide)
    * [2.1.2. Actions và vai trò kích hoạt trong Streaming](#212-actions-và-vai-trò-kích-hoạt-trong-streaming)
      * [2.1.3. Lợi thế của Lazy Evaluation đối với Fault Tolerance](#213-lợi-thế-của-lazy-evaluation-đối-với-fault-tolerance)
    * [2.2. Catalyst Optimizer và Tungsten Execution Engine](#22-catalyst-optimizer-và-tungsten-execution-engine)
  * [3. Kiến trúc tích hợp: Apache Kafka và Spark Structured Streaming](#3-kiến-trúc-tích-hợp-apache-kafka-và-spark-structured-streaming)
    * [3.1. Thiết lập Kafka Source: Cấu hình và chiến lược đọc](#31-thiết-lập-kafka-source-cấu-hình-và-chiến-lược-đọc)
      * [3.1.1 Chiến lược quản lý Offset (startingOffsets)](#311-chiến-lược-quản-lý-offset-startingoffsets)
    * [3.2. Cấu trúc dữ liệu và giải mã Payload](#32-cấu-trúc-dữ-liệu-và-giải-mã-payload)
  * [4. Kỹ thuật Parsing dữ liệu phức tạp: Xử lý JSON lồng nhau](#4-kỹ-thuật-parsing-dữ-liệu-phức-tạp-xử-lý-json-lồng-nhau)
    * [4.1. Tầm quan trọng của Schema tường minh (Explicit Schema)](#41-tầm-quan-trọng-của-schema-tường-minh-explicit-schema)
    * [4.2. Chiến lược parsing và làm phẳng (Flattening)](#42-chiến-lược-parsing-và-làm-phẳng-flattening)
      * [4.2.1. Sử dụng `explode` cho mảng](#421-sử-dụng-explode-cho-mảng)
      * [4.2.2. Xử lý lỗi Parsing (Handling Bad Records)](#422-xử-lý-lỗi-parsing-handling-bad-records)
  * [5. Xử lý thời gian trong Streaming: Windowing và Watermarking](#5-xử-lý-thời-gian-trong-streaming-windowing-và-watermarking)
    * [5.1. Các mô hình cửa sổ (Windowing models)](#51-các-mô-hình-cửa-sổ-windowing-models)
    * [5.2. Cơ chế WaterMarking: Quản lý trạng thái và dữ liệu đến muộn](#52-cơ-chế-watermarking-quản-lý-trạng-thái-và-dữ-liệu-đến-muộn)
    * [5.3. Tác động của Output Mode đến Watermarking](#53-tác-động-của-output-mode-đến-watermarking)
  * [6. Output Sinks, Triggers và quản lý checkpoint](#6-output-sinks-triggers-và-quản-lý-checkpoint)
    * [6.1. Triggers: Kiểm soát nhịp độ pipeline](#61-triggers-kiểm-soát-nhịp-độ-pipeline)
    * [6.2. Checkpointing: Cốt lõi của Fault Tolerance](#62-checkpointing-cốt-lõi-của-fault-tolerance)
  * [7. Đảm bảo ngữ nghĩa Exactly-Once (chính xác một lần)](#7-đảm-bảo-ngữ-nghĩa-exactly-once-chính-xác-một-lần)
  * [9. Các vấn đề thực tiến và chiến lược tối ưu hoá nâng cao](#9-các-vấn-đề-thực-tiến-và-chiến-lược-tối-ưu-hoá-nâng-cao)
    * [9.1. Quản lý State Store: RocksDB vs HDFSBacked](#91-quản-lý-state-store-rocksdb-vs-hdfsbacked)
    * [9.2. Xử lý dữ liệu bị lệch (Data Skew)](#92-xử-lý-dữ-liệu-bị-lệch-data-skew)
    * [9.3. Giám sát và Observability](#93-giám-sát-và-observability)
  * [10. Kết luận](#10-kết-luận)
<!-- TOC -->


## 1. Sự chuyển dịch kiến trúc: Từ Bt

### 1.1. Sự tiến hoá của hệ sinh thái Spark: DStreams so với Structured Streaming

Lịch sử phát triển của Apache Spark ghi nhận hai giai đoạn chính trong công nghệ steamming: 
Thế hệ đầu tiên, Spark Streaming (thường được gọi là DStreams - Discretized Streams), xây dựng dựa trên RDD
(Resilient Distributed Datasets). DStreams hoạt động bằng cách chia nhỏ luồng dữ liệu đầu vào thành các RDD nhỏ
và xử lý chúng như các tác vụ batch cực nhỏ. Mặc dù mô hình này mang lại tính chịu lỗi cao, nhưng nó gặp khó khăn 
trong việc tối ưu hoá hiệu suất và phức tạp trong việc xử lý thời gian sự kiện (event-time processing) 
do thiếu sự hỗ trợ của Catalyst Optimizer. 

Structured Streaming ra đời như một sự tái định hình, được xây dựng trực tiếp tên Spark SQL Engine. 
Ý tưởng cốt lõi và mang tính cách mạng của Structed Streaming là coi luồng dữ liệu trực tiếp (live data stream) như 
một bảng không giới hạn (**unbounded table**) được thêm dữ liệu liên tục (append-only). Mô hình này cho phép các kỹ sư dữ liệu 
diễn đạt các tính toán streaming bằng cùng một ngữ nghĩa  (semantics) như các truy vấn batch trên bảng tĩnh. Khi dữ liệu mới đến, 
Spark SQL Engine sẽ chạy truy vấn dưới dạng một truy vấn gia tăng (incremental query), tự động tính toán trạng thái và cập nhật 
kết quả cuối cùng. Sự thống nhất này không chỉ đơn giản hoá việc phát triển API mà còn tận dụng tối đa sức mạnh của Project Tungsten 
và Catalyst Optimizer để tối ưu hoá mã nguồn.

## 2. Nền tảng lý thuyết: Pyspark DataFrame API và cơ chế thực thi
Để làm chủ Structured Streaming, trước hết cần phải có sự hiểu biết sâu sắc về các nguyên lý hoạt động của Pyspark Dataframe API, 
vì Structured Streaming thực chất là DataFrame API vận hành trên dữ liệu động. Cơ chế thực thi của Spark được thiết kế để tối ưu 
hoá thông lượng (throughput) và khả năng chịu lỗi thông qua mô hình tính toán đồ thị không chu trình có hướng (DAG - Directed Acyclic Graph).

### 2.1. Lazy Evaluation: Chiến lược tối ưu hoá và hiệu năng

#### 2.1.1. Phân loại Transformations: Narrow và Wide

Hiểu rõ sự khác biệt giữa các loại Transformations là chìa khoá để tối ưu hiệu suất pipeline. 
* **Narrow Transformations (Biến đổi hẹp):** Đây là các hoạt động mà dữ liệu cần thiết để tính toán một phân vùng (partition) 
kết quả chỉ nằm trong một phân vùng duy nhất của DataFrame cha. Các ví dụ điển hình bao gồm: `map`, `filter`, `select`, và `withColumn`. 
Đặc điểm quan trọng của narrow transformation là chúng có thể được thực hiện cục bộ trên từng node mà không cần di chuyển dữ liệu qua mạng. 
Spark có khả năng "pipeline" các transformation này, nghĩa là gộp nhiều bước xử lý (ví dụ: filter rồi map) thành một stage duy nhất để giảm thiểu I/O. 

### 2.1.2. Actions và vai trò kích hoạt trong Streaming
Trong mô hình batch, các lệnh như `collect()`, `count()`, `show()` đóng vai trò là Actions kích hoạt tính toán. Tuy nhiên, trong Stuctured Streaming, 
khái niệm Action được chuyển dịch sang việc khởi động luồng dữ liệu thông quan phương thức `start()`. Một truy vấn streaming thường được định nghĩa 
bằng một `writeStream`, thiết lập sink đích (ví dụ: Kafka, Console, Parquet) và chế độ xuất (output mode). Khi `start()` được gọi, Spark sẽ kích hoạt 
một tiến trình nền liên tục thực thi kế hoạch logic đã được định nghĩa trên dữ liệu mới đến. 

#### 2.1.3. Lợi thế của Lazy Evaluation đối với Fault Tolerance
Ngoài việc tối ưu hoá hiệu năng, Lazy Evaluation còn đóng vai trò quan trọng trong khả năng chịu lỗi (fault tolerance). Bằng cách lưu trữ đồ thị dòng dõi
(lineage graph) của các transformations thay vì dữ liệu vật lý, Spark có thể tái tạo lại bất kỳ phân vùng dữ liệu nào bị mất do lỗi phần cứng bằng cách 
chia lại chia lại các biến đổi từ dữ liệu gốc. Trong Structured Streaming, điều này kết hợp với cơ chế checkpoint và replayable sources (như Kafka) để đảm 
bảo tính toàn vẹn dữ liệu ngay cả khi hệ thống gặp sự cố nghiêm trọng. 

### 2.2. Catalyst Optimizer và Tungsten Execution Engine

Sức mạnh của Structed Streaming nằm ở khả năng sử dụng Catalyst Optimizer. Khi người dùng định nghĩa một truy vấn streaming, Catalyst sẽ thực hiện một loạt các bước tối ưu hoá:
1. **Analysis:** Phân giải các tên cột và kiểu dữ liệu dựa trên schema. 
2. **Local Optimization:** Áp dụng các quy tắc như đẩy bộ lọc xuống nguồn (filter pushdown) để giảm lượng dữ liệu cần đọc, hoặc gộp các phép chiếu (project pruning).
3. **Physical Planning:** Tạo ra nhiều kế hoạch vật lý và chọn kế hoạch hiệu quả nhất dựa trên mô hình chi phí (cost model).
4. **Code Generation:** Sử dụng Tungsten để sinh mã bytecode Java tối ưu, loại bỏ overhead của việc gọi hàm ảo và tận dụng bộ nhớ Cache của CPU. 


## 3. Kiến trúc tích hợp: Apache Kafka và Spark Structured Streaming
Apache Kafka đóng vai trò là hệ thần kinh trung ương trong các kiến trúc dữ liệu hiện đại, cung cấp khả năng lưu trữ log phân tán, độ trễ thấp và thông lượng cao. 
Việc tích hợp Spark với Kafka đòi hỏi sự hiểu biết sâu sắc về cấu hình để đảm bảo ngữ nghĩa xử lý "chính xác một lần" (exactly-once semantics) và hiệu suất ổn định. 

### 3.1. Thiết lập Kafka Source: Cấu hình và chiến lược đọc
Để khởi tạo một luồng đọc Kafka, Spark sử dụng `readStream` với format là `kafka`. Tuy nhiên, sự phức tạp nằm ở các tuy chỉnh cấu hình điều khiển hành vi đọc. 

| Tùy chọn Cấu hình | Giá trị Mẫu | Mô tả Kỹ thuật và Tác động |
|-------------------|-------------|----------------------------|
| `kafka.bootstrap.servers` | `host1:9092,host2:9092` | Danh sách các broker để thiết lập kết nối ban đầu. Nên cung cấp ít nhất 2 host để đảm bảo tính sẵn sàng cao (High Availability). |
| `subscribe` | `topic_a,topic_b` | Danh sách các topic cần tiêu thụ. Đây là phương thức phổ biến nhất, cho phép Spark tự động phát hiện các partition của topic. |
| `subscribePattern` | `sensor_.*` | Sử dụng Regex để đăng ký nhiều topic. Rất hữu ích trong các hệ thống IoT động nơi topic mới được tạo ra liên tục. |
| `minPartitions` | `10` | Đề xuất Spark chia nhỏ các Kafka partition thành nhiều Spark partition hơn để tăng tính song song (parallelism) khi đọc. |
| `maxOffsetsPerTrigger` | `10000` | Giới hạn số lượng bản ghi tối đa được đọc trong mỗi micro-batch. Đây là cấu hình quan trọng để kiểm soát áp lực lên hệ thống (backpressure) và ngăn chặn OOM khi có sự gia tăng đột biến về lượng dữ liệu. |

#### 3.1.1 Chiến lược quản lý Offset (startingOffsets)

Việc quản lý offset quyết định điểm khởi đầu của pipeline và ảnh hưởng trực tiếp đến tính toàn vẹn dữ liệu. Spark cung cấp các tuỳ chọn `earliest`, `latest` và cấu hình JSON chi tiết.
Một điểm cực kỳ quan trọng cần lưu ý tham số `startingOffsets` chỉ có tác dụng khi **khởi động truy vấn lần đầu tiên** (khi chưa có checkpoint). Nếu một checkpoint đã tồn tại, Spark sẽ 
**luôn** ưu tiên sử dụng offset được lưu trong checkpoint để đảm bảo tính liên tục, bỏ qua `startingOffsets`. 

Việc hiểu sai cơ chế này là nguyên nhân phổ biến dẫn đến mất dữ liệu hoặc xử lý trùng lặp trong quá trình phát triển. Ví dụ, nếu developer thay đổi `startingPoints` từ `latest` sang `earliest` 
với hy vọng xử lý dữ liệu cũ nhưng không xoá thư mục checkpoint, Spark vẫn sẽ tiếp tục đọc từ vị trí `latest` đã lưu trước đó. 

### 3.2. Cấu trúc dữ liệu và giải mã Payload
Dữ liệu đọc từ Kafka được Spark biểu diễn dưới dạng một Dataframe với schema cố định, trong đó quan trọng nhất là cột `key` và `value` ở dạng nhị phân (`binary`). 

```
# Schema chuẩn của Kafka Source DataFrame
root

|-- key: binary (nullable = true)
|-- value: binary (nullable = true)
|-- topic: string (nullable = true)
|-- partition: integer (nullable = true)
|-- offset: long (nullable = true)
|-- timestamp: timestamp (nullable = true)
|-- timestampType: integer (nullable = true)
```

Bước đầu tiên trong hầu hết các pipeline xử lý là giải mã (deserialize) các cột nhị phân này. Thông thường, dữ liệu được mã hoá dưới dạng chuỗi UTF-8 (JSON, CSV, Text) hoặc Avro. 
Việc ép kiểu (casting) sang String là bắt buộc trước khi áp dụng các hàm parsing. 

```
# Chuyển đổi Binary sang String để xử lý tiếp
df_raw = spark.readStream.format("kafka")...load()
df_string = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
```

## 4. Kỹ thuật Parsing dữ liệu phức tạp: Xử lý JSON lồng nhau
Trong thực tế, dữ liệu Streaming hiếm khi ở dạng phẳng. JSON với cấu trúc lồng nhau (nested structures) và mảng (arrays) là định dạng trao đổi dữ liệu phổ biến. 
Spark cung cấp công cụ mạnh mẽ để xử lý loại dữ liệu này, nhưng việc sử dụng chúng đòi hỏi sự chính xác về schema. 

### 4.1. Tầm quan trọng của Schema tường minh (Explicit Schema)

Mặc dù Spark hỗ trợ suy luận Schema (schema interface), việc sử dụng nó trong streaming production là không khuyến khích (và thường bị tắt mặc định).
Lý do là chi phí I/O để đọc mẫu dữ liệu suy luận và rủi ro schema thay đổi (schema drift) dẫn đến lỗi pipeline. Thay vào đó, việc định nghĩa `StructType`
tường minh mang lại hiệu suất cao hơn và sự an toàn về kiểu dữ liệu. 

```Python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

complex_schema = StructType([
    StructField("items", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
    ), True)
])
```


### 4.2. Chiến lược parsing và làm phẳng (Flattening)
Hàm `from_json` là công cụ cốt lõi để chuyển đổi chuỗi JSON thành cấu trúc `Struct` của Spark. 
Sau khi Parse, thách thức tiếp theo là làm phẳng cấu trúc này để dễ dàng truy vấn SQL.  

#### 4.2.1. Sử dụng `explode` cho mảng
Khi dữ liệu chứa mảng (ví dụ: danh sách sản phẩm trong một đơn hàng), việc giữ nguyên mảng sẽ gây khó khăn cho các phép tính tổng hợp (aggregation).
Hàm `explode` cho phép "bùng nổ" một hàng chứa mảng thành nhiều hàng, một hàng chứa mảng thành nhiều hàng, mỗi hàng chứa một phần tử của mạng, trong khi
lặp lại các dữ liệu các dữ liệu của cột khác. 

```Python
from pyspark.sql.functions import from_json, col, explode

# 1. Parse JSON string thành Struct
df_parsed = df_string.select(from_json(col("value"), complex_schema).alias("data"))

# 2. Truy cập trường lồng nhau và Explode mảng
# Biến đổi: 1 Order (có n items) -> n dòng dữ liệu
df_flattened = df_parsed.select(
    col("data.event_id"),
    col("data.user.id").alias("user_id"),
    explode(col("data.items")).alias("item") # Explode mảng items
)

# 3. Truy cập các trường bên trong struct 'item' sau khi explode
final_df = df_flattened.select(
    "event_id",
    "user_id",
    col("item.sku"),
    col("item.qty")
)
```

Kỹ thuật này biến đổi dữ liệu phân cấp (hierarchical) thành dữ liệu quan hệ (relational), 
cho phép áp dụng `groupBy` và `sum` trên từng sản phẩm riêng biệt. 

#### 4.2.2. Xử lý lỗi Parsing (Handling Bad Records)

Trong môi trường streaming, dữ liệu (malformed JSON) là điều không thể tránh khỏi. Hàm `from_json` hỗ trợ các chế độ xử lý lỗi thông qua tuỳ chọn `mode`:

* `PERMISSIVE` (mặc định): Đặt các trường lỗi thành `null` và ghi nhận bản ghi lỗi vào một cột riêng (nếu được cấu hình `columnNameOfCorruptRecord`). Điều này giúp pipeline không bị crash. 
* `FAILFAST`: Dừng ngay pipeline khi gặp lỗi. Phù hợp trong giai đoạn kiểm thử nhưng nguy hiểm trong production. 
* `DROPMALFORMED`: Tự động loại bỏ các bản ghi không parse được. 

## 5. Xử lý thời gian trong Streaming: Windowing và Watermarking

Một trong những thách thức lớn nhất của stream processing là sự sai lệch giữa **Event Time** (thời gian sự kiện xảy ra) và **Processing Time** (thời gian hệ thống nhận và xử lý sự kiện).
Structured Streaming được thiết kế để xử lý dựa trên Event Time, cho phép kết quả chính xác ngay cả khi dữ liệu đến muộn hoặc không theo thứ tự. 

### 5.1. Các mô hình cửa sổ (Windowing models)

Spark hỗ trợ nhiều loại cửa sổ thời gian để thực hiện các phép tính tổng hợp (aggregation).

| Loại Cửa Sổ | Đặc điểm | Trường hợp Sử dụng | Cú pháp Python |
|--------------|----------|-------------------|----------------|
| **Tumbling Window** | Kích thước cố định, không chồng lấn, liên tiếp nhau. | Báo cáo doanh thu mỗi giờ, đếm số lượt truy cập mỗi 5 phút. | `window(col("ts"), "10 mins")` |
| **Sliding Window** | Kích thước cố định, trượt theo khoảng thời gian nhỏ hơn kích thước (có chồng lấn). | Tính trung bình trượt (moving average), phát hiện xu hướng trong 1 giờ qua cập nhật mỗi 5 phút. | `window(col("ts"), "10 mins", "5 mins")` |
| **Session Window** | Kích thước động, dựa trên hoạt động của người dùng (timeout). | Phân tích phiên người dùng web, gom nhóm các sự kiện liên tiếp. (Có sẵn từ Spark 3.2+). | `session_window(col("ts"), "30 mins")` |

### 5.2. Cơ chế WaterMarking: Quản lý trạng thái và dữ liệu đến muộn

Trong các phép tính window (ví dụ: đếm số sự kiện trong 1 giờ), Spark phải giữ trạng thái (state) của các cửa sổ trong bộ nhớ. 
Nếu không có cơ chế dọn dẹp, bộ nhớ sẽ tràn vì Spark không biết khi nào một cửa sổ "đã xong" và sẽ không nhận thêm dữ liệu nữa. 
**Watermarking** giải quyết vấn đề này bằng cách xác định một thời gian chấp nhận độ trễ. 

Watermark được tính toán động: `Watermark = Max(Event Time) - Delay Threshold`.

Ví dụ: Nếu `Delay Threshold` là 10 phút và sự kiện mới nhất hệ thống thấy là 12:30, thì Watermark là 12:20. 
* Hệ quả 1: Mọi dữ liệu có thời gian thực sự (event time) nhỏ hơn 12:20 đến sau thời điểm này sẽ bị coi là "quá muộn" và bị loại bỏ (dropped)
để bảo vệ tính đúng đắn của trạng thái bộ nhớ.
* Hệ quả 2: Các cửa sổ kết thúc trước 12:20 sẽ được coi là đã hoàn tất (finalized), trạng thái của chúng sẽ được xoá khỏi bộ nhớ, và kết quả sẽ được xuất ra Sink (tuy thuộc vào output model).

```Python
# Ví dụ áp dụng Watermark và Sliding Window
windowed_counts = events_df \
   .withWatermark("timestamp", "10 minutes") \
   .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("type")
    ).count()
```

### 5.3. Tác động của Output Mode đến Watermarking
Mối quan hệ giữa Wartermark và Output Mode là cực kỳ quan trọng:
* **Append Mode:** Spark chỉ xuất kết quả của một cửa sổ ra Sink khi và chỉ khi Watermark đã vượt qua thời điểm kết thúc của cửa sổ đó. 
Điều này đảm bảo rằng kết quả được xuất ra là cuối cùng và sẽ không thay đổi nữa. Nhược điểm là độ trễ xuất kết quả sẽ bằng ít nhất là thời
gian của cửa sổ cộng với thời gian trễ (delay threshold). 
* **Update Mode:** Spark xuất kết quả cập nhật của cửa sổ ngay khi có dữ liệu mới (trigger), không cần chờ watermark. Khi watermark đi qua, 
trạng thái cũ được dọn dẹp nhưng không có kết quả nào được xuất thêm. Chế độ này giảm độ trễ hiển thị nhưng yêu cầu Sink phải hỗ trợ cập nhật (upsert)
nếu muốn dữ liệu nhất quán. 

## 6. Output Sinks, Triggers và quản lý checkpoint
Giai đoạn cuối cùng của pipeline là kiểm soát tần suất xử lý và nơi lưu trữ kết quả. 

### 6.1. Triggers: Kiểm soát nhịp độ pipeline
Trigger xác định thời điểm Spark thực thi việc xử lý dữ liệu mới.

* `processingTime` **(micro-batch mặc định)**: Spark chạy các batch theo chu kỳ cố định (ví dụ: "30 seconds"). Nếu batch trước xử lý lâu hơn 30s, 
batch tiếp theo sẽ chạy ngay lập tức sau đó. Đây là chế độ an toàn và phổ biến nhất. 
* `AvailableNow`: Đây là cách cải tiến vượt bậc so với chế độ `Once` (đã deprecated). Trong chế độ này, Spark sẽ xử lý toàn bộ dữ liệu có sẵn trong Kafka 
kể từ lần chạy trước, nhưng sẽ tự động chia nhỏ lượng dữ liệu này thành nhiều micro-batch (dựa trên `maxOffsetPerTrigger`) để tránh tràn bộ nhớ (OOM). Sau khi
xử lý hết, job sẽ dừng lại. Đây là lựa chọn tối ưu cho các tác vụ batch định kỳ trên nguồn streaming, giúp tiết kiệm chi phí tính toán đám mây. 
* `Continuous` **(Thử nghiệm)**: Chế độ này hướng tới độ trễ thấp ở mức mili-giây bằng cách khởi chạy task dài hạn (long-running tasks) thay vì lập lịch lại các
task cho mỗi batch. Tuy nhiên, nó hỗ trợ hạn chế các phép biến đổi SQL và chỉ đảm bảo "At-least-once".

### 6.2. Checkpointing: Cốt lõi của Fault Tolerance
Checkpointing là cơ chế sống còn để Spark Structured Streaming đạt được khả năng phục hồi. 
Spark liên tục ghi lại metadata của quá trình xử lý (offset range của mỗi batch, trạng thái commit)
và dữ liệu state (cho các phép aggregation) vào một thư mục trên hệ thống tệp tin phân tán (HDFS/S3).

Khi một job bị lỗi (crash) và được khởi động lại với cùng `checkpointLocation`, Spark sẽ: 
1. Đọc file commit cuối cùng để xác định batch nào đã hoàn thành. 
2. Lấy offset của batch tiếp theo cần xử lý. 
3. Khôi phục trạng thái bộ nhớ (state store) từ checkpoint. 
4. Tiếp tục xử lý từ đúng điểm đã dừng, đảm bảo không mất dữ liệu. 

Một cảnh báo quan trọng trong vận hành: **Không thay đổi logic của truy vấn stateful (ví dụ: thay đổi schema aggregation, thay đổi kích thước window)** trên cùng một thư mục checkpoint.
Sự không tương thích giữa logic mới và state đã lưu sẽ dẫn đến lỗi hoặc kết quả sai lệch không thể đoán được trước. 

## 7. Đảm bảo ngữ nghĩa Exactly-Once (chính xác một lần)

Mục tiêu "chén thánh" (Holy Grail) của hệ thống phân tán là Exactly-Once Processing. Structured Streamming đạt được điều này thông qua sự phối hợp chặt chẽ giữa Source, Engine và Sink.
1. **Replayable Source (Kafka):** Kafka cho phép đọc lại dữ liệu chính xác từ một offset cụ thể. 
2. **Deterministic Engine (Spark):** Với cùng một dữ liệu đầu vào và cùng một logic. Spark luôn tạo ra cùng một kết quả. Checkpointing đảm bảo Spark biết chính xác đã xử lý đến đâu. 
3. **Idempotent Sink** Đây là yếu tố quyết định cuối cùng. Sink phải có khả năng xử lý việc ghi lặp lại (trong trường hợp Spark retry do lỗi mạng) mà không tạo ra bản ghi trùng lặp. 
* **File Sink:** Spark sử dụng cơ chế đổi tên file nguyên tử (atomic rename) hoặc transaction log (như trong Delta Lake) để đảm bảo file chỉ xuất hiện khi batch hoàn tất thành công. 
* **Kafka Sink:** Khi ghi ngược lại Kafka, Spark sử dụng tính năng "Idempotent Producer" của Kafka. Cần thiết lập cấu hình `kafka.enable.idempotence` hoặc `processing.guarantee="exactly_once"`
trong `writeStream` để Spark quản lý transaction ID, đảm bảo tin nhắn không bị nhân bản ngay cả khi có retry.

## 9. Các vấn đề thực tiến và chiến lược tối ưu hoá nâng cao
### 9.1. Quản lý State Store: RocksDB vs HDFSBacked

Mặc định, Spark lưu trữ trạng thái (state) trong bộ nhớ JVM của executor (HDFSBackedStateStoreProvider). Khi state trở nên quá lớn
(hàng trăm keys trong aggregation hoặc join), áp lực lên Garbage Collection (GC) sẽ tăng cao, gây ra độ trễ (latency spikes) và thậm chí là crash executor. 

Databricks và các phiên bản Spark mới (Open Source 3.2+) hỗ trợ RocksDB State Store. RocksDB lưu trữ state trên đĩa local (SSD) của executor thay vì trên Java Heap, 
giúp giảm thiểu GC pause và cho phép lưu trữ lượng state lớn gấp 100 lần so với mặc định. Cấu hình kích hoạt: `spark.sql.streaming.stateStore.providerClass = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"`

### 9.2. Xử lý dữ liệu bị lệch (Data Skew)
Trong các phép `groupBy` hoặc `join` (Wide Transformation), dữ liệu được shuffle dựa trên key. Nếu phân bố dữ liệu không đều (ví dụ: một `category` chiếm 80% số đơn hàng), một executor sẽ phải xử lý lượng lớn
dữ liệu khổng lồ trong khi các executor khác nhàn rỗi. Đây gọi là hiện tượng Data Skew. Giải pháp trong streaming là **Salting**: Thêm một số ngẫu nhiên vào key trước khi group, sau đó thực hiện group hai lần (lần 1 theo key + salt, lần 2 theo key gốc)
để phân tán tải đều hơn trên cluster. 

### 9.3. Giám sát và Observability

Để vận hành hệ thống trong production, việc giám sát là bắt buộc. Spark cung cấp `StreamingQueryListener` để lắng nghe các sự kiện về tiến độ của query. 
Các metrics quan trọng cần theo dõi trong đối tượng `StreamingQueryProgress`:
* `inputRowsPerSecond`: Tốc độ dữ liệu đến từ Kafka. 
* `processRowsPerSecond`: Tốc độ xử lý của Spark. Nếu `process` < `input` kéo dài, hệ thống đang bị quá tải (lagging) và cần scale up để tối ưu hoá. 
* `stateOperators` kích thước bộ nhớ state sử dụng. Sự gia tăng không ngừng của metric này có thể là dấu hiệu của việc watermark không hoạt động hiệu quả hoặc logic state bị rò rỉ (memory leak). 


## 10. Kết luận

PySpark Structured Streaming đại diện cho đỉnh cao của công nghệ xử lý luồng hiện đại, cung cấp sự cân bằng hoàn hảo giữa độ trễ thấp, thông lượng cao và tính dễ sử dụng thông qua DataFrame API thống nhất. Bằng cách kết hợp khả năng lưu trữ bền vững của Kafka với cơ chế quản lý trạng thái thông minh và ngữ nghĩa exactly-once của Spark, 
các doanh nghiệp có thể xây dựng những hệ thống phản hồi thời gian thực tin cậy, từ phát hiện gian lận tài chính đến tối ưu hóa chuỗi cung ứng tức thì. Chìa khóa thành công không chỉ nằm ở việc viết mã, mà còn ở việc thấu hiểu sâu sắc các cấu hình hệ thống: từ trigger, watermark, đến checkpointing và quản lý bộ nhớ state.