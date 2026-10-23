import os
import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title='Country Atlas', page_icon='🌍', layout='wide')
st.title('🌍 Country Atlas')
st.caption('ประชากรและพื้นที่ของประเทศและเขตปกครอง • GeoNames / Worldometer / World Bank')


def query(sql):
    with psycopg2.connect(host=os.environ['DB_HOST'],user=os.environ['DB_USER'],
                          password=os.environ['DB_PASSWORD'],dbname=os.environ['DB_NAME']) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return pd.DataFrame(cur.fetchall(),columns=[d[0] for d in cur.description])


st.sidebar.button('รีเฟรชข้อมูล')
try:
    countries = query('SELECT * FROM countries ORDER BY name')
    observations = query('SELECT * FROM country_observations ORDER BY name,source')
    runs = query('SELECT * FROM pipeline_runs ORDER BY checked_at DESC LIMIT 30')
    statuses = query('SELECT * FROM pipeline_sources ORDER BY checked_at DESC LIMIT 90')
except psycopg2.Error:
    st.info('รอฐานข้อมูลพร้อมและรัน DAG country_comparison ใน Airflow ก่อน')
    st.stop()

dashboard, comparison, pipeline = st.tabs(['Dashboard','เปรียบเทียบแหล่งข้อมูล','Pipeline status'])
with dashboard:
    if countries.empty:
        st.info('ยังไม่มีข้อมูล กรุณารัน DAG ที่ http://localhost:8080')
    else:
        regions = st.sidebar.multiselect('ภูมิภาค', sorted(countries.region.dropna().unique()))
        search = st.sidebar.text_input('ค้นหาชื่อหรือ ISO3')
        source = st.sidebar.selectbox('ต้องมีข้อมูลจากแหล่ง', ['ทั้งหมด','geonames','worldometer','worldbank'])
        view = countries.copy()
        if regions:
            view = view[view.region.isin(regions)]
        if search:
            view = view[view.name.str.contains(search,case=False,regex=False) | view.iso3.str.contains(search,case=False,regex=False)]
        if source != 'ทั้งหมด':
            view = view[view.sources.apply(lambda values: source in values)]
        a,b,c = st.columns(3)
        a.metric('ประเทศ / เขตปกครอง',len(view))
        b.metric('ประชากรรวม (พันล้านคน)',f'{view.population.sum()/1_000_000_000:,.3f}')
        c.metric('แหล่งที่พบ',len({s for values in view.sources for s in values}))
        st.warning('ประชากรอาจมีปีอ้างอิงต่างกัน และเขตปกครองอาจซ้อนกับประเทศแม่ ผลรวมนี้ไม่ใช่จำนวนประชากรโลกอย่างเป็นทางการ')
        st.plotly_chart(px.bar(view.nlargest(15,'population'),x='name',y='population',color='region',
                              title='15 ประเทศ / เขตปกครองที่มีประชากรมากที่สุดในตัวกรอง'),use_container_width=True)
        st.dataframe(view,use_container_width=True,hide_index=True)
        st.download_button('ดาวน์โหลด CSV',view.to_csv(index=False).encode('utf-8-sig'),'countries.csv','text/csv')
with comparison:
    if not observations.empty:
        code = st.selectbox('ประเทศ (ISO3)',sorted(observations.iso3.unique()))
        selected = observations[observations.iso3==code]
        st.dataframe(selected,use_container_width=True,hide_index=True)
        st.plotly_chart(px.bar(selected,x='source',y='population',color='source',
                              title=f'ประชากร {code} แยกตามแหล่งข้อมูล'),use_container_width=True)
        st.caption('ปีว่างหมายถึงแหล่งข้อมูลไม่ระบุปี ห้ามถือว่าเป็นปีปัจจุบัน / พื้นที่อาจใช้คำนิยามต่างกัน')
with pipeline:
    st.subheader('ประวัติการรัน')
    st.dataframe(runs,use_container_width=True,hide_index=True)
    st.subheader('สถานะแต่ละแหล่ง')
    st.dataframe(statuses,use_container_width=True,hide_index=True)
    st.caption('degraded = รวมข้อมูลได้แต่บางแหล่งล้มเหลว • quality_failed = ไม่ผ่านคุณภาพและไม่โหลดข้อมูล')
