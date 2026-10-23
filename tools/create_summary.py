"""Generate the three-page submission using real screenshots in evidence/."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tmp/pdf_runtime'))
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import HexColor
from reportlab.lib.utils import ImageReader
from PIL import Image

data = json.loads((ROOT/'submission.json').read_text(encoding='utf-8'))
pdfmetrics.registerFont(TTFont('Thai', 'C:/Windows/Fonts/tahoma.ttf'))
pdfmetrics.registerFont(TTFont('ThaiBold', 'C:/Windows/Fonts/tahomabd.ttf'))
out = ROOT/'output/pdf'/f"{data['ordinal']}_{data['student_id']}_summary.pdf"
out.parent.mkdir(parents=True,exist_ok=True)
c = canvas.Canvas(str(out),pagesize=(595.28,841.89))
c.setTitle('Country Atlas - 6621601000')
c.setAuthor(data['student_name'])


def text(value,x,y,size=10,bold=False,color='#243447'):
    c.setFillColor(HexColor(color))
    c.setFont('ThaiBold' if bold else 'Thai',size)
    c.drawString(x,y,value,shaping=True)


def lines(value,y,size=10,width=515,leading=16):
    # Break by measured glyph width; keep explicit section/line boundaries.
    for paragraph in value.split('\n'):
        line = ''
        for char in paragraph:
            if pdfmetrics.stringWidth(line+char,'Thai',size)>width:
                text(line,40,y,size)
                y -= leading
                line = ''
            line += char
        text(line,40,y,size)
        y -= leading
    return y


def header(page,title):
    c.setFillColor(HexColor('#0c4660'))
    c.rect(0,786,596,56,fill=1,stroke=0)
    text('COUNTRY ATLAS',40,812,17,True,'#ffffff')
    text(title,40,795,10,False,'#ffffff')
    text(f'02739452 | ลำดับที่ 16 | 6621601000 | หมู่ 700',40,768,9)
    text(f'{page} / 3',522,23,9)
    text('นิธิวดี โพธิ์พันเหมือน',40,23,9)


def image(name,y,height=220):
    path = ROOT/'evidence'/name
    with Image.open(path) as im:
        width,h = im.size
    scale = min(515/width,height/h)
    rendered_w,rendered_h=width*scale,h*scale
    c.drawImage(ImageReader(str(path)),40,y-rendered_h,width=rendered_w,height=rendered_h)
    return y-rendered_h


header(1,'1. ภาพรวมระบบและหลักฐาน Airflow')
y=lines('หัวข้อ: ระบบรวมและเปรียบเทียบข้อมูลประชากรและพื้นที่ของประเทศทั่วโลก\n'
        'ทำได้ถึงระดับ 3 พร้อมโบนัสหน้าสถานะ pipeline บนเว็บ\n'
        'Airflow 3 TaskGroup ดึงขนาน -> schema กลาง -> ตรวจคุณภาพ -> UPSERT PostgreSQL\n'
        'GeoNames 250 + Worldometer 234 + World Bank 217 = 701 observations รวม 251 รหัสไม่ซ้ำ',745,10)
text('Graph view: แหล่งข้อมูล 3 กลุ่มและ BranchPythonOperator',40,y-3,10,True)
y=image('graph.png',y-13,242)
text('Grid view: รอบปกติสำเร็จ; รอบทดสอบข้อมูลว่างล้มเหลวตามที่ออกแบบ',40,y-20,10,True)
y=image('grid.png',y-30,242)
y=lines('Primary key: countries(iso3), observations(source, iso3); รันซ้ำแล้วจำนวนแถวคงเดิม\n'
        'Repo: '+data['github_repo'],y-19,9,leading=15)
c.linkURL(data['github_repo'],(40,y+1,550,y+18),relative=0)
c.showPage()

header(2,'2. เว็บ dashboard และผลการทดสอบ')
y=lines('เว็บ Streamlit อ่าน PostgreSQL โดยตรง มี KPI ตัวกรองภูมิภาค/ชื่อประเทศ/แหล่งข้อมูล\n'
        'มีตาราง กราฟประชากร หน้าเปรียบเทียบรายประเทศ และหน้า Pipeline status',745,10)
text('หน้าเว็บจริง: KPI และตัวกรอง',40,y-3,10,True)
y=image('dashboard.png',y-13,214)
text('กราฟและตารางข้อมูลจาก PostgreSQL',40,y-18,10,True)
y=image('dashboard-table.png',y-28,214)
y=lines('ผลทดสอบ: รอบปกติ 251 ประเทศ/เขตปกครอง และ 701 observations\n'
        'ทดสอบ UPSERT: รันซ้ำแล้วคง 251 / 701 แถว ไม่มีแถวซ้ำตาม primary key\n'
        'จำลอง World Bank ล่ม: อีก 2 แหล่งรวมได้ 250 รหัส สถานะ degraded\n'
        'จำลอง merge ว่าง: ไป quarantine, DAG failed และข้อมูลหลักคงเดิม\n'
        'extract ทั้ง 3 เริ่มเวลา 13:26:58 น. พร้อมกัน (23 ต.ค. 2569 เวลาไทย)\n'
        'เก็บปีอ้างอิงเมื่อมี; ปีว่างไม่สมมติเป็นปีปัจจุบัน และผลรวมไม่ใช่ประชากรโลกทางการ',y-20,9,leading=15)
c.showPage()

header(3,'3. ปัญหา วิธีแก้ และการวางแผน')
y=745
sections=[
('ปัญหา 1: API เดิมเลิกให้บริการ',
 'อาการ: REST Countries v3.1 ตอบ HTTP 200 แต่ไม่มีรายการประเทศ\n'
 'หาสาเหตุ: อ่าน response พบข้อความ deprecated และทดสอบ v5 ได้ HTTP 401\n'
 'วิธีแก้: เปลี่ยนเป็น Worldometer ตรวจ robots.txt แล้ว scrape HTML แทน ได้ 234 รายการ'),
('ปัญหา 2: ชื่อประเทศและภาษาไม่ตรงกัน',
 'อาการ: จับคู่ Russia กับ ISO ไม่ได้ และชื่อ Côte d\'Ivoire แสดงอักขระผิด\n'
 'หาสาเหตุ: เทียบชื่อเว็บกับ pycountry พบชื่อเรียกต่างกัน และตรวจ encoding ของ HTML\n'
 'วิธีแก้: อ่าน HTML เป็น UTF-8 และทำ alias map เช่น Russia -> RUS; ไม่เดารหัสที่จับคู่ไม่ได้'),
('ปัญหา 3: Docker BuildKit กับเส้นทางภาษาไทย',
 'อาการ: build แจ้ง header มี non-printable ASCII และ Compose หา project name ไม่ได้\n'
 'หาสาเหตุ: ตรวจข้อความผิดพลาดเทียบชื่อโฟลเดอร์ปัจจุบันที่เป็นภาษาไทย\n'
 'วิธีแก้: ระบุ name: country-comparison และใช้ DOCKER_BUILDKIT=0 จากนั้น build สำเร็จ'),
('การแบ่งเวลาและลำดับงาน',
 'แผน 180 นาที: ตรวจแหล่งข้อมูล 20 / DAG และฐานข้อมูล 70 / เว็บ 35 / ทดสอบ 25 / PDF และส่ง 30\n'
 'ทำขั้นต่ำให้ครบก่อน แล้วเพิ่ม 3 แหล่ง UPSERT คุณภาพข้อมูล และหน้า pipeline\n'
 'ช่วงพัฒนาและทดสอบจริงเริ่มประมาณ 13:14 น.; เวลาที่เหลือใช้ตรวจ repo/PDF และส่งก่อน 16:00 น.\n'
 'ตัดแผนที่ ระบบสมาชิก และ Tableau/Power BI เพราะเพิ่มขั้นตอนเผยแพร่และสิทธิ์เข้าดู\n'
 'เลือก Streamlit เพื่อรันซ้ำจาก Docker Compose และอ่านฐานข้อมูลโดยตรงได้สะดวก'),
('แหล่งข้อมูล ข้อจำกัด และการส่ง',
 'GeoNames: https://www.geonames.org/countries/ (CC BY 4.0; ระบุที่มาใน README)\n'
 'Worldometer: https://www.worldometers.info/world-population/population-by-country/\n'
 'World Bank: https://api.worldbank.org/v2/ (ตัวชี้วัด SP.POP.TOTL)\n'
 'ตรวจ robots.txt ก่อนดึงทุกครั้ง ไม่ใช้ Selenium และไม่เก็บข้อมูลส่วนบุคคล\n'
 'ค่าประชากรต่างปี/คำนิยามอาจต่างกัน; พื้นที่ใช้ GeoNames; เก็บ URL และเวลาอัปเดต\n'
 'Repo: '+data['github_repo']+'\n'
 'ส่งลิงก์ public repo และ PDF ใน Classroom ก่อน 16:00 น.; ห้าม commit .env หรือ API key')]
for title,body in sections:
    text(title,40,y,11,True,'#0c4660')
    y=lines(body,y-21,9.2,leading=15)-13
assert y>40, f'Page 3 overflow: {y}'
c.save()
print(out)
