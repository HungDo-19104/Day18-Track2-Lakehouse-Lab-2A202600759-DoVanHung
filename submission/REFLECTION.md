# Reflection — Top 5 Lakehouse Anti-Patterns

**Họ tên:** Đỗ Văn Hưng
**Lab:** Day 18 — Data Lakehouse Architecture (Track 2)

## Anti-pattern dễ vướng nhất: Bronze Swamp

Anti-pattern mà team tôi có khả năng cao nhất vướng phải là **Bronze Swamp** — biến
Bronze layer thành một "đầm lầy" dữ liệu thô không có cấu trúc, không có schema
enforcement, và không có lineage tracking.

Lý do thực tế: trong giai đoạn đầu của dự án, áp lực deliver nhanh khiến team dễ
chọn con đường đơn giản là dump toàn bộ raw event vào Bronze mà không định nghĩa
schema rõ ràng, không validate, không ghi lại nguồn gốc dữ liệu. Kết quả là Silver
layer nhận dữ liệu hỗn độn — JSON malformed lẫn với valid record, duplicate tràn
lan — và mọi bug ở downstream đều phải trace ngược lên Bronze để debug.

Lab này đã minh họa trực tiếp hệ quả: NB4 phải dùng `ROW_NUMBER()` dedup và
`model IS NOT NULL` filter ngay ở Silver vì Bronze chứa cả retry duplicate lẫn
malformed rows. Nếu Bronze có schema enforcement ngay từ đầu (như NB1 đã chứng
minh là có thể làm được với `write_deltalake` + `schema_mode`), chi phí cleanup
ở Silver sẽ thấp hơn nhiều.

Bài học: enforce schema tại ingestion point, không phải tại transformation point.
