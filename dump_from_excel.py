import pandas as pd 
import psycopg2 
from sqlalchemy import engine
master_path=r'D:\practice\Kemis_masters.xlsx'
path=r'D:\practice\institute.csv'
fun_path=r'D:\practice\institute'
df=pd.read_csv(path)
xl=pd.ExcelFile(master_path)
#engine=engine.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/demo')
email_list=df.email.to_list()
conn=psycopg2.connect('dbname=demo user=postgres password=postgres')
cursor=conn.cursor()
try:
    def institue_reg():
              for index,row in df.iterrows():
                email=row['email']
                cursor.execute('select distinct email from dlc_tenant where email=%s',(email,))
                result=cursor.fetchone()
                if  result is  None:
                   cursor.execute(f"""
                   call sp_upload_institutions('{fun_path}')
                   """)
    print('Institution complete')
    # for email in email_list:
    #       tnt_code=pd.read_sql(f"select tnt_code from dlc_tenant where email={email}",engine=conn)
    def master_reg():
        for sheet in xl.sheet_names:
              sheet_name=sheet
              #ACADEMIC
              if sheet_name=='dlc_academic':
               academic=pd.read_excel(master_path,sheet_name='dlc_academic')
               for row in academic.itertuples(index=False):
                    row
                    email=f"'{row[1]}'"
                    print(email)
                    
                    cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
                    tnt_code=cursor.fetchone()[0]
                    print(tnt_code)
                    if not tnt_code:
                     print('starting insertion into dlc_academic')
                     cursor.execute("""
                       insert into dlc_academic
                     (academic_name,tenant_code,academic_startdate,academic_enddate,academic_description)
                       values(%s,%s,%s,%s,%s)""",
                       (row.academic_name,tnt_code,
                       row.academic_startdate,
                       row.academic_enddate,row.academic_description))
    print('Insertion of master completed')
except Exception as e:
    print('Insertion not completed properly and the error is: {e}')
institue_reg()
master_reg()
conn.commit()
cursor.close()
conn.close()
