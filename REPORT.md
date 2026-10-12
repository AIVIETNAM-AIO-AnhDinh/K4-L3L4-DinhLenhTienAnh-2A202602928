# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đinh Lệnh Tiến Anh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602928` |
| GitHub | `AIVIETNAM-AIO-AnhDinh` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02689` (MSSV có chữ `A` nên lệnh trong GUIDE báo lỗi `int()`; tôi bỏ ký tự không phải số: `2202602928 % 26 = 6` → phần tử thứ 6 của `list-trips`) |

## Kết quả cuối

- `dataset_version` của bạn: `6d80a6cb7a5b`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **729 / 132 / 159** (tổng 1020 khung, 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bước 1 nạp 3021 dòng (A) và 3234 dòng (B); lần nạp thứ hai báo `already present` cho cả video lẫn telemetry, và `cli history bronze.raw_telemetry_a` chỉ có **một commit** (version 0, `rows_added=3021`) vì file được nhận diện bằng sha256 (A `636ba1a9…`, B `bfa62703…`).

Khác biệt A và B thấy ngay ở bronze:
1. **Tên cột**: `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer` (A) so với `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering` (B).
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. **Bàn đạp**: A là tỷ lệ 0..1 (`gas=0.028`), B là phần trăm (`throttle_pct=2.0`).
5. **Đồng hồ**: B chạy nhanh hơn video `telemetry_clock_offset_ms = 1500` (A = 0), khai báo trong `sources.json` và lưu ở `bronze.video_files`.

Giữ mỗi nguồn một bảng và để mọi cột là chữ vì bronze phải là **bản ghi nguyên gốc, chỉ thêm**: nếu ép kiểu ngay thì ô trống, `250` km/h hay một định dạng lạ sẽ làm hỏng cả lần nạp hoặc bị biến thành NULL im lặng, và ta mất dữ liệu gốc để làm lại. Hai schema khác nhau thì gộp sớm buộc phải đoán cách đổi đơn vị ngay lúc nạp; tách ra cho phép silver sửa luật chuẩn hoá rồi **chạy lại từ bronze** bất cứ lúc nào (schema-on-read), và mỗi dòng còn giữ `_source_file, _batch_id, _row_no` để truy vết.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai model staging đưa cả hai nguồn về cùng cột `drive_id, ts, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`:
- `stg_telemetry_a`: chỉ ép kiểu (ISO UTC → timestamp, chữ → double).
- `stg_telemetry_b`: đổi tên cột, `spd_kmh / 3.6` → m/s, `throttle_pct / 100` và `brake_pct / 100`, và thời gian `to_timestamp((timestamp_ms - offset_ms) / 1000)`, trong đó `offset_ms = 1500` lấy từ `bronze.video_files` (chứ không gõ cứng).
- Sau đó `silver_telemetry_classified` gộp `union all by name`, đánh dấu bản sao bằng `row_number()` và gán một `reject_reason` cho mỗi dòng.

Bằng chứng đúng: dòng B đầu tiên trong CSV là `1759933801509` = 14:30:01.509 UTC; sau khi trừ 1500 ms, `bat_dau` của B là **`2025-10-08 14:30:00.009`**, khớp với `video_start_utc = 14:30:00` (verify: "drive B clock offset removed", lệch 9 ms). Tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), hợp lý, và test `in_range 0..70 m/s` PASS. Ngoài ra, sạch + cách ly = nhận về: A `2968 + 53 = 3021`, B `3181 + 53 = 3234`.

| drive_id | duplicate | speed_missing | speed_out_of_range | tổng cách ly |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

(3 dòng `speed_out_of_range` mỗi drive đều là 250 m/s = 900 km/h.) Dòng lỗi đi vào `silver_telemetry_rejects` kèm lý do, `_batch_id` và `_row_no`, thay vì bị xoá, vì: (1) **kiểm toán được**: luôn đếm được "nhận bao nhiêu, sạch bao nhiêu, loại bao nhiêu, vì sao"; (2) nếu luật sai (ví dụ ngưỡng 70 m/s quá chặt cho một loại xe) thì ta sửa luật và lấy lại dòng mà không phải nạp lại; (3) tỷ lệ cách ly tăng đột biến là tín hiệu một cảm biến hay firmware đang hỏng. Xoá im lặng thì mất cả ba điều đó.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng cách giữa hai mẫu liên tiếp > 1000 ms) mô tả **một hiện tượng có thật**: cảm biến mất tín hiệu. Dữ liệu vẫn đúng, chỉ thưa hơn; ở bài này mỗi drive có đúng 1 khoảng trống > 1 s (dài nhất 3073 ms ở A, 3124 ms ở B). Dừng cả pipeline vì một đoạn mất sóng thì quá đắt, nên test này là `warn`: báo cho người vận hành, còn phía sau tự xử lý (khung hình nào không có telemetry trong `sync_tol_ms = 150` thì bị cờ `mat_dong_bo`, 7 khung mỗi drive). Ngược lại, `in_range 0..70 m/s` thất bại nghĩa là **có giá trị vật lý không thể xảy ra trong bảng sạch**, tức luật làm sạch đã hỏng. Nếu cho đi tiếp thì dữ liệu sai sẽ vào gold và mart, nên test này là `error` và chặn các task phía sau.

Thí nghiệm `break-gate` (`report/evidence/break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 thì 6 dòng 250 m/s (3 của A, 3 của B) không còn bị cách ly mà lọt vào `silver_telemetry`. Kết quả `dbt test` là `PASS=5 WARN=1 ERROR=1`: test **`in_range_silver_telemetry_speed_mps__70__0` bắt được 6 dòng** và `gate_failed = true`. Khôi phục luật thì trở lại `PASS=6 WARN=1 ERROR=0`. Bài học: cổng chất lượng là một **lớp phòng thủ độc lập** với logic làm sạch. Luật làm sạch (một biến trong `dbt_project.yml`) có thể bị ai đó sửa nhầm, còn test viết riêng với ngưỡng riêng vẫn bắt được, và vì là `error` nên DAG dừng trước gold. Đó là cách nó ngăn dữ liệu xấu lan xuống.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `report/evidence/idempotency.json` (trip `trip-00003`, 60 khung):
- Cách của pipeline (Delta `overwrite` với predicate `trip_id = 'trip-00003'`): **trước 1246 dòng, sau hai lần chạy lại vẫn 1246 dòng**; dấu vân tay bảng `cb6118dbb0b8` → `cb6118dbb0b8`, không đổi.
- Cách sai (`append`, chạy 2 lần): trip 60 khung thành **120 dòng**, nhân đôi.

`cli history silver.frames` cho thấy mỗi lần ghi trip là một commit `WRITE` có `predicate: trip_id = '…'` (ví dụ version 25 `rows_added=23` cho `trip-02689` khi chạy lại ở bước 5). Overwrite theo partition an toàn hơn "DELETE rồi INSERT" vì nó là **một commit nguyên tử** trong transaction log của Delta: file mới được ghi xong rồi mới có một commit vừa gỡ file cũ của partition vừa thêm file mới. Nếu tiến trình chết giữa chừng (như trip của tôi ở bước 5) thì không có commit nào, người đọc vẫn thấy phiên bản cũ đầy đủ, và file rác chưa commit bị bỏ qua (`VACUUM` dọn sau). Với xoá rồi ghi, lỗi giữa hai thao tác để lại một partition **rỗng hoặc ghi một nửa** mà người đọc đồng thời nhìn thấy được. Ngoài ra, overwrite theo predicate chỉ đụng đến một trip nên backfill hay chạy lại một trip không ảnh hưởng 25 trip còn lại, và luôn có time travel để quay lại version trước.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 / 602 | 8 / 644 |
| 2 (đang dùng) | 149 / 602 | 52 / 644 |
| 6 | 363 / 602 | 244 / 644 |

(đủ dải từ `threshold.json`: A 60, 109, 149, 270, 363, 438 và B 8, 24, 52, 137, 244, 350 với ngưỡng 0, 1, 2, 4, 6, 8 bit.)

**Đen và tối**: khung đen là **lỗi cảm biến** (độ sáng trung bình < `dark_luma = 12`; thực tế 6 khung ở A và 4 ở B đều có brightness = **0.0**), không có thông tin gì nên bị loại khỏi gold. Khung tối (12 ≤ độ sáng < `dark_soft = 40`) là **điều kiện thật**: đường hầm, bóng râm. Pipeline gắn cờ `is_dark` nhưng **giữ lại** (A 4 khung, độ sáng 35.8–39.6; B **61** khung, 12.8–39.6). Tôi đồng ý: mô hình lái xe phải học được cảnh thiếu sáng, bỏ chúng sẽ làm dataset thiên về trời sáng và mô hình sẽ hỏng đúng lúc khó nhất. Cờ vẫn hữu ích để cân bằng hoặc phân tích lỗi theo điều kiện ánh sáng. Điều tôi chưa thích: ranh giới 12 là cứng, nên một khung đường hầm rất tối (≈ 12.8 ở B) chỉ cách "bị loại" một chút.

**Chọn ngưỡng**: tôi giữ **2 bit**. Ngưỡng 0 chỉ bắt khung giống hệt (A 60), bỏ sót camera đứng hình có nhiễu nén. Verify vẫn bắt 11/12 khung đứng hình ở ngưỡng 2. Ngưỡng 6 loại 363/602 khung của A (60%) và 244/644 của B, tức là đã xoá cả những khung khác nhau thật khi xe chạy. Ngưỡng phụ thuộc tốc độ: ở 2 khung/giây, xe chậm thì hai khung liền nhau gần như giống hệt. Trên dữ liệu của tôi (ngưỡng 2), tỷ lệ khung bị cờ trùng là **50.9% khi < 10 km/h** (55/108), nhưng chỉ 11.4% ở 10–40 km/h và **8.9% khi ≥ 70 km/h**. Vì vậy ngưỡng lý tưởng nên **giảm khi xe chạy nhanh** (khung khác nhau nhiều, chỉ coi trùng khi thật sự đứng hình), hoặc tốt hơn là lấy mẫu theo quãng đường thay vì theo thời gian. Drive A có nhiều khung trùng hơn B (149 so với 52) vì A chạy chậm hơn và có đoạn camera đứng hình 6 giây.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `report/evidence/leakage.json`: trong 1020 khung đáng dùng thuộc 26 trip, chia **theo khung hình** (ngẫu nhiên 70/15/15) làm **26/26 trip bị chia cắt**, và cả **26 trip có khung ở cả train lẫn test**. Chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt; dbt test chống rò rỉ và verify "no trip is in two splits" đều PASS.

Điểm kiểm thử đẹp giả tạo vì ở 2 khung/giây, hai khung liền nhau của cùng một trip gần như giống hệt nhau (cùng đường, cùng ánh sáng, cùng thời tiết, cùng tốc độ). Chia theo khung thì gần như mọi khung test đều có "anh em sinh đôi" trong train: mô hình chỉ cần **nhớ** cảnh là đoán đúng, chứ không cần **tổng quát hoá**. Điểm test lúc đó đo khả năng nhớ, không đo hiệu năng trên một con đường hay một chuyến đi mới, mà đó lại chính là điều xe tự lái gặp khi chạy thật. Chia theo trip (cảnh) đảm bảo test chứa những cảnh mô hình chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version = sha256(danh sách "frame_id|split" đã sắp xếp)[:12]` (`pipeline/publish.py`). Đó là **băm của nội dung**: cùng tập khung và cùng cách chia thì ra cùng version, bất kể chạy lúc nào, ở máy nào, bao nhiêu lần. Một khung bị thêm, bớt hay đổi split thì version đổi. Nếu dùng ngày giờ hay số đếm thì hai lần chạy giống hệt nhau sẽ có hai version khác nhau (không biết chúng có thực sự khác không), còn hai lần chạy khác nhau lại có thể trùng số đếm giữa các máy. Băm nội dung cũng giúp thư mục `gold/datasets/<version>/` không bao giờ bị ghi đè bởi một dataset khác.

Kết quả của tôi: `step all --reset`, `cli export-dataset` chạy lại, DAG Airflow, và lần chạy lại sau bài tập sập đều cho cùng **`6d80a6cb7a5b`**, và `manifest_sha256` cũng giữ nguyên `16be5dde…`. Khi tôi đổi tạm `LAB_STUDENT_ID` sang `2A202600000` và chạy lại bước 4, version thành **`07b0fa163fe8`**, split thành train/val/test = 729/161/130 (của tôi là 729/132/159). Đặt lại MSSV thì version quay về `6d80a6cb7a5b`. Lý do: trong `gold_dataset_frames.sql`, thứ tự trip trong mỗi drive là `md5(trip_id || LAB_STUDENT_ID)`, nên MSSV thay đổi **trip nào rơi vào split nào** (số trip mỗi split vẫn là 18/4/4). Dữ liệu và luật lọc như nhau, nhưng tập `frame_id|split` khác nên băm khác. Vì thế giảng viên có thể chạy lại với MSSV của tôi và đối chiếu đúng version.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lineage` trong `lake/gold/datasets/6d80a6cb7a5b/manifest.json` (bản ghi bởi lần chạy DAG Airflow lúc 10:05:56 UTC):

```json
"bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
"silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
"input_video_sha256": {"drive_a": "636ba1a98dfc…7e9aa", "drive_b": "bfa62703f645…e9fdc"}
```

Đọc ra: dataset này được tạo từ **hai video gốc có sha256 như trên**, telemetry thô A và B ở version 0 của bảng bronze, bảng trips version 0, và bảng `silver.frames` ở **version 25** (26 commit ghi theo từng trip, 0..25; trước đó, khi tôi đã chạy thí nghiệm idempotent, cùng dataset này ghi `silver.frames: 27`, nhưng version dataset vẫn là `6d80a6cb7a5b` vì nội dung không đổi). Manifest cũng ghi `rules` (bỏ `is_black, is_blurry, is_near_dup, not sync_ok`; split "per trip, stratified by drive, md5 order") và `manifest_sha256`.

Ba tháng sau, để tái tạo đúng dataset cần: (1) hai file video đúng sha256 cùng các CSV gốc, vốn còn nguyên ở bronze (chỉ thêm); (2) **time travel** Delta về đúng các version trong `lineage`; (3) đúng phiên bản mã/luật (git commit của `dbt/` và các biến `dup_hamming=2, sync_tol_ms=150, max_speed_mps=70, split 0.70/0.15`); (4) **MSSV `2A202602928`** (salt của split). Sau đó so `dataset_version` và `manifest_sha256` để chứng minh tái tạo đúng. Thiếu sót: manifest chưa ghi git commit của mã và giá trị các biến dbt, nên tôi phải dựa vào lịch sử git.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
