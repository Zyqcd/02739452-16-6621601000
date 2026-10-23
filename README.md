# Country Atlas: ประชากรและพื้นที่ของประเทศทั่วโลก

ข้อสอบปลายภาครายวิชา 02739452 รวมข้อมูลจาก GeoNames, Worldometer และ World Bank
ด้วย Apache Airflow ลง PostgreSQL และอ่านฐานข้อมูลโดยตรงด้วยเว็บ Streamlit

## เริ่มใช้งาน

ต้องมี Docker Desktop ที่เปิด Linux containers และ Docker Compose v2
คัดลอก `.env.example` เป็น `.env` และเปลี่ยนรหัสผ่านทั้งสองค่า
ใช้รหัสผ่านตัวอักษร/ตัวเลขที่ไม่มีอักขระพิเศษใน URL

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

หาก BuildKit แจ้ง non-printable ASCII กับเส้นทางภาษาไทย:

```powershell
$env:DOCKER_BUILDKIT='0'
docker compose up -d --build
```

- Airflow: http://localhost:8080 ลงชื่อเข้าใช้ด้วยค่าจาก `.env`
- เปิด DAG `country_comparison` แล้วกด Trigger; เปิดสวิตช์หากต้องการรันทุกวัน
- เว็บ: http://localhost:8501 กดรีเฟรชหลัง DAG ทำงานเสร็จ
- pgAdmin: host `localhost`, port `5433`, database/user/password ตาม `.env`
- หยุดโดยเก็บข้อมูล: `docker compose down`

ไม่ต้องติดตั้ง Airflow หรือ requirements.txt บน Windows โดยตรง เพราะรันใน Docker Linux
`requirements.txt` ระบุ dependencies ภาพรวม ส่วน Dockerfile แยก dependencies ของแต่ละ service

## แหล่งข้อมูล

1. https://www.geonames.org/countries/ - scrape HTML ด้วย requests + BeautifulSoup
2. https://www.worldometers.info/world-population/population-by-country/ - scrape HTML ประชากรพร้อมปีอ้างอิง
3. https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=20000&mrnev=1
   - ประชากรล่าสุดที่ไม่เป็นค่าว่าง พร้อมปี และ country metadata จาก API เดียวกัน

ก่อนร้องขอแต่ละครั้งระบบอ่าน robots.txt ด้วย User-Agent ที่ระบุชื่อโครงงาน
หากถูกห้ามหรืออ่าน robots.txt ไม่สำเร็จ (ยกเว้น 404/410) จะหยุดแหล่งนั้น
ไม่หลบเลี่ยงการบล็อก ไม่ใช้ Selenium ไม่เก็บข้อมูลส่วนบุคคล
GeoNames ใช้ CC BY 4.0: https://www.geonames.org/about.html
ตรวจสอบเงื่อนไขใช้งานของแหล่งข้อมูลก่อนเผยแพร่ชุดข้อมูลต่อ

## DAG และ schema

`initialize -> [geonames, worldometer, worldbank].(extract -> transform)
-> merge -> quality_gate -> load / quarantine -> done`

TaskGroup ทั้งสามเป็นอิสระให้ scheduler รันขนานกันได้
ความผิดพลาดจากเครือข่าย/robots ถูกบันทึกใน `pipeline_sources` และแหล่งที่เหลือทำต่อ
การตรวจคุณภาพกำหนดอย่างน้อย 100 ISO3 ที่ไม่ซ้ำ และแถวผิดรูปแบบไม่เกิน 5%
หากไม่ผ่านจะไป quarantine บันทึกเหตุผลและทำให้ DAG ล้มเหลวโดยไม่แก้ตารางข้อมูล

- `country_observations`: primary key `(source, iso3)` เก็บ provenance ของแต่ละแหล่ง
- `countries`: primary key `iso3` รวมข้ามแหล่งด้วย UPSERT
- `pipeline_runs`, `pipeline_sources`: ประวัติสถานะที่หน้าเว็บอ่านได้โดยตรง

ลำดับเลือกประชากร: World Bank -> Worldometer -> GeoNames
พื้นที่เลือกค่าที่ไม่ว่างตามลำดับเดียวกัน (World Bank ชุดนี้ไม่มีพื้นที่)
ปีที่แหล่งไม่ระบุเก็บ NULL ห้ามสมมติเป็นปีปัจจุบัน
ภูมิภาคใช้แหล่งที่มีข้อมูลก่อน ทั้งนี้ชื่อกลุ่มจากแต่ละแหล่งอาจไม่เหมือนกัน
ข้อมูลเก่าจากแหล่งที่ล่มไม่ถูกนำมา merge ในรอบใหม่ แต่เก็บ observations เก่าไว้พร้อม fetched_at
ประเทศที่ไม่พบในรอบใหม่ยังอยู่ในตารางพร้อม updated_at เดิมเพื่อไม่ลบประวัติโดยพลการ
ผลรวมอาจซ้อนประเทศกับเขตปกครอง จึงไม่เรียกผลรวมว่าประชากรโลก

## สิ่งที่ต้องทดสอบและเก็บหลักฐาน

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags trigger country_comparison
```

รันซ้ำสองรอบ ตรวจจำนวนแถว `countries` และ `country_observations` ไม่เพิ่มจากข้อมูลเดิม
จำลองหนึ่งแหล่งล่มผ่าน Trigger DAG > Configuration:
`{"simulate_failed_source":"worldbank"}` ต้องเป็น degraded และโหลดอีกสองแหล่งได้
ทดสอบ quality gate ผ่าน Configuration `{"simulate_empty_merge":true}`
ต้องไป quarantine, DAG failed และไม่มีการแก้ตาราง countries / country_observations
การทดสอบใช้ flags ที่ระบุชัดเจนเท่านั้น รอบปกติไม่ใส่ configuration
อย่ารัน `airflow dags test` พร้อม scheduler เพราะอาจแย่งกันทำ task ใน run เดียวกัน
เก็บภาพ Graph view, Grid view และเว็บจริง ห้ามใช้ภาพจำลองแทนผลการรัน


## ผู้จัดทำ

นิธิวดี โพธิ์พันเหมือน รหัสนิสิต 6621601000 หมู่ 700
ลำดับที่ 16 ข้อมูลชื่อส่งงานและ repo เก็บใน submission.json
แหล่งเดิม REST Countries ถูกแทนที่ด้วย Worldometer เนื่องจาก v3.1 ยุติบริการและ v5 ต้องใช้ key
หากข้อความจองยังระบุแหล่งเดิม ให้ปรับรายชื่อแหล่งตามนี้โดยคงขอบเขตเดิม
