AppFlow Custom Connector for Amazon Redshift (JDBC)

Mô tả ngắn: Dự án này triển khai một AWS AppFlow Custom Connector (nguồn và đích) dựa trên JDBC để đọc/ghi dữ liệu với Amazon Redshift. Connector chạy trên AWS Lambda (Java 11), dùng AWS AppFlow Custom Connector SDK và lấy thông tin xác thực/kết nối từ AWS Secrets Manager.

Nội dung chính:
- Hỗ trợ chế độ: nguồn (SOURCE) và đích (DESTINATION).
- Liệt kê bảng (entities), mô tả cột (describe entity), truy vấn dữ liệu có phân trang, và ghi dữ liệu (INSERT/UPDATE/UPSERT).
- Xác thực tuỳ biến (Custom Auth) qua Secrets Manager với các khoá: `driver`, `hostname`, `port`, `database`, `username`, `password`.

1) Kiến trúc & Luồng xử lý
- Lambda Handler: `org.custom.connector.jdbc.handler.JDBCConnectorLambdaHandler::handleRequest` kết hợp 3 handler của SDK:
  - Metadata: `JDBCConnectorMetadataHandler` – liệt kê bảng và mô tả cột qua JDBC/Redshift.
  - Record: `JDBCConnectorRecordHandler` – truy vấn (SELECT) và ghi (INSERT/UPDATE/UPSERT) theo yêu cầu từ AppFlow.
  - Configuration: `JDBCConnectorConfigurationHandler` – khai báo chế độ, phiên bản API, xác thực, và kiểm tra kết nối.
- Client Factory: `JDBCClientFactory` chọn `RedshiftClient` dựa vào `driver` trong Secret.
- Secrets: `SecretsManagerHelper` đọc secret theo ARN AppFlow cung cấp, trả về map thông số kết nối JDBC.

2) Yêu cầu tiên quyết
- AWS Account với quyền tạo Lambda, IAM, AppFlow, Secrets Manager, và (tuỳ chọn) ECR.
- Java 11 JDK và Maven 3.8+.
- AWS CLI cấu hình sẵn (`aws configure`).
- SAM CLI (tuỳ chọn) nếu deploy qua CloudFormation (`template.yml`).
- Docker (tuỳ chọn) nếu build/deploy Lambda bằng container image.

3) Cấu trúc thư mục (rút gọn)
```
Appflow-Custom-Connector-Redshift/
  pom.xml
  template.yml               # SAM template (Zip deployment, handler Java)
  Dockerfile                 # Container runtime cho Lambda Java 11
  src/main/java/org/custom/connector/jdbc/
    handler/                 # Lambda + SDK handlers
    client/                  # JDBC client, Secrets Manager helper
    config/                  # Khai báo connector, auth params
    utils/                   # Logger/Context cho test
  src/test/java/...          # Bài test ví dụ gọi handler
```

4) Cấu hình Secrets Manager (bắt buộc)
- Tạo một secret (kiểu Plaintext hoặc theo định dạng mà AppFlow Custom Auth tạo) chứa các khoá sau:
```json
{
  "driver": "redshift",
  "hostname": "<cluster-endpoint>",
  "port": "5439",
  "database": "<db_name>",
  "username": "<db_user>",
  "password": "<db_password>"
}
```
- Lưu ý IAM trong `template.yml` cho phép đọc các secret có tên theo mẫu `appflow!<ACCOUNT_ID>-*`. Hãy đảm bảo secret của bạn tương thích hoặc chỉnh lại policy nếu cần.

5) Build (Maven)
- Câu lệnh:
```powershell
mvn clean package
```
- Kết quả:
  - JAR đóng gói: `target/appflow-custom-jdbc-connector-1.0.jar` (shade plugin).
  - Thư viện phụ thuộc: `target/dependency/` (phục vụ Dockerfile nếu dùng container).

6) Triển khai
6.1. SAM/CloudFormation (Zip artifact)
- `template.yml` trỏ `CodeUri` tới file JAR đã build. Triển khai nhanh:
```powershell
sam deploy --guided
```
- Chọn Stack Name, Region, xác nhận quyền IAM. Sau khi deploy xong, ghi lại Lambda ARN để dùng trong AppFlow.

6.2. Lambda Container image (tuỳ chọn)
- Build image dựa trên Dockerfile (runtime AWS Lambda Java 11):
```powershell
docker build -t appflow-redshift:latest .
```
- Đẩy image lên ECR rồi tạo Lambda từ image đó (bạn cần ECR repo và quyền push). Handler trong container được cấu hình sẵn trong `CMD` của Dockerfile.

7) Đăng ký và sử dụng trong AWS AppFlow
- Đăng ký connector riêng (Private connector) trỏ tới Lambda function ARN đã triển khai.
- Tạo Connection Profile (Custom Auth): nhập ARN của Secret đã tạo (AppFlow sẽ truyền ARN này xuống Lambda).
- Tạo Flow:
  - Nguồn (SOURCE): chọn connector này, entity là tên bảng Redshift; đặt `selectedFieldNames`, bộ lọc (nếu có) và `maxResults` để phân trang.
  - Đích (DESTINATION): ghi vào bảng đích, chọn `operation` là INSERT/UPDATE/UPSERT và (nếu UPDATE) thiết lập `idFieldNames`.

8) Chi tiết chức năng
- List entities: duyệt `DatabaseMetaData` để liệt kê các bảng.
- Describe entity: đọc `pg_table_def` để trả về danh sách cột và kiểu dữ liệu (map sang `FieldDataType`).
- Query data: tạo câu lệnh `SELECT` với danh sách cột đã chọn, `WHERE` từ `filterExpression` (nếu có), phân trang bằng `OFFSET ... LIMIT ...`. Trả kết quả JSON từng bản ghi.
- Write data:
  - INSERT và UPDATE được hỗ trợ.
  - UPSERT dùng nhánh xử lý tương tự INSERT/REPLACE theo mã nguồn hiện tại; hãy kiểm thử với Redshift của bạn và điều chỉnh nếu cần vì Redshift không có lệnh `REPLACE` nguyên thuỷ.
- Validate credentials: thử mở kết nối JDBC từ thông số trong Secret.

9) Kiểm thử
- Các bài test mẫu gọi trực tiếp Lambda handler (không assert) nằm ở `src/test/java/...`:
```powershell
mvn -Dtest=*Test test
```
- Để test thật cần cung cấp ARN secret hợp lệ (thay `your-secret-arn`) và cấu hình AWS CLI/credential hoạt động.

10) Lưu ý & Giới hạn
- `retrieveData` hiện chưa triển khai (trả về null khi không hợp lệ).
- `filterExpression` đi thẳng vào câu lệnh SQL – cần đảm bảo an toàn/kiểm soát đầu vào khi cấu hình Flow.
- Ánh xạ kiểu dữ liệu Redshift → AppFlow SDK được đơn giản hoá, bạn nên xem xét chỉnh sửa nếu có cột đặc thù.
- UPSERT cho Redshift có thể cần chiến lược MERGE/transaction riêng; hiện code dùng phương án đơn giản để minh hoạ.

11) Giấy phép
- Mã nguồn có gắn MIT-0 như trong tệp LICENSE và header file.

12) Tham khảo
- AWS AppFlow Custom Connector SDK (Java).
- AWS Lambda Java 11 runtime.
- Amazon Redshift JDBC driver.

Hỗ trợ nhanh
- Handler: `org.custom.connector.jdbc.handler.JDBCConnectorLambdaHandler::handleRequest`
- Build: `mvn clean package`
- Deploy (SAM): `sam deploy --guided`
- Custome connector source redshift in Appflow
