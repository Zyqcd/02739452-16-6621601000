# ผลตรวจระบบจริง 23 ตุลาคม 2569

ผู้จัดทำ: นิธิวดี โพธิ์พันเหมือน 6621601000 หมู่ 700 ลำดับที่ 16

## แหล่งข้อมูลและฐานข้อมูล

- GeoNames HTML: 250 รายการ
- Worldometer HTML: 234 รายการ
- World Bank API: 217 รายการ
- country_observations: 701 แถว, primary key (source,iso3)
- countries: 251 แถว และ 251 รหัสไม่ซ้ำ, primary key iso3
- หลังรันซ้ำจำนวนแถวคง 251 / 701
- อ่านข้อมูลจริงด้วย SQL ผ่าน service postgres
- Airflow list-import-errors: No data found (ไม่มี import error)

## Airflow runs (เวลา run_id เป็น UTC)

| Run | ผล |
|---|---|
| scheduled__2026-10-22T00:00:00+00:00 | success, รวม 251 |
| manual__2026-10-23T06:23:33+00:00 | success, รวม 251 |
| manual__2026-10-23T06:25:33+00:00 | จำลอง World Bank ล่มผ่าน conf, degraded, อีก 2 แหล่งรวม 250 |
| manual__2026-10-23T06:26:14+00:00 | จำลอง merge ว่าง, quality_failed, quarantine, ไม่แก้ข้อมูลหลัก |
| manual__2026-10-23T06:26:56+00:00 | success, รวม 251 |

มีรอบทดสอบ CLI เวลา 06:20 UTC เพิ่มเติม ซึ่ง scheduler กับ dags test แย่งกันทำ task
เกิด retry ก่อนสำเร็จ; จึงใช้ scheduler trigger + configuration ในการยืนยันรอบถัดมา

เวลา extract รอบปกติสุดท้ายจาก task_instance:

| Task | เริ่ม UTC | จบ UTC |
|---|---|---|
| geonames.extract | 06:26:58.910778 | 06:27:02.303635 |
| worldometer.extract | 06:26:58.911784 | 06:27:01.510939 |
| worldbank.extract | 06:26:58.916220 | 06:27:00.152861 |

ช่วงเวลาซ้อนกัน ยืนยันการทำงานขนานด้วย TaskGroup + LocalExecutor

## เว็บและเอกสาร

ตรวจหน้า dashboard จริง: 251 รายการ, 3 แหล่ง, KPI, ตัวกรอง, กราฟ, ตาราง
ตรวจหน้าเปรียบเทียบ THA: มีข้อมูลจากทั้ง 3 แหล่งพร้อมปีและ URL
ภาพ graph.png, grid.png, dashboard.png, dashboard-table.png มาจาก browser ของระบบจริง
ผู้จัดทำจัดทำ PDF เอง โดยใช้ภาพและผลทดสอบข้างต้นเป็นข้อมูลประกอบ

## ข้อจำกัด

แหล่งข้อมูลอาจเปลี่ยนรูปแบบหรือหยุดให้บริการได้; ระบบบันทึกสถานะแหล่งล่ม
ปีข้อมูลไม่เท่ากันและเขตปกครองอาจซ้อนกับประเทศแม่ ไม่ใช้ผลรวมเป็นประชากรโลกอย่างเป็นทางการ
รหัสพิเศษของแหล่ง เช่น XKX ถูกเก็บเป็น source country code ไม่อ้างว่าเป็น ISO ที่รับรองทั้งหมด
รายการที่ไม่พบในรอบใหม่ยังอยู่พร้อม updated_at เก่า
