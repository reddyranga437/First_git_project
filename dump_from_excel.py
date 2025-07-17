import pandas as pd 
import psycopg2 
from sqlalchemy import engine
master_path=r'D:\practice\Kemis_masters.xlsx'
path=r'D:\practice\institute.csv'
fun_path=r'D:\practice\institute'
df=pd.read_csv(path)
df.where(pd.notna(df),None,inplace=True)
print(df)
xl=pd.ExcelFile(master_path)
print(xl)
#engine=engine.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/demo')
email_list=df.email.to_list()
conn=psycopg2.connect('dbname=demo user=postgres password=postgres')
cursor=conn.cursor()
print(email_list)
<<<<<<< HEAD
=======
#functions start
>>>>>>> 4c916aada2a2fb7cc91cc604e1f8731e63817771
def institue_reg():
  try:
          for index,row in df.iterrows():
                email=row['email']
                print(email)
                cursor.execute('select distinct email from dlc_tenant where email=%s',(email,))
                result=cursor.fetchone()
                print(result)
                if  result is  None:
                   cursor.execute(f"""
                   call sp_upload_institutions('{fun_path}')
                   """)
                   print('Institute inserted')
                else:
                    print('Institute already exists')
          print('Institution complete')
  except Exception as e:
     print(f'Institute insertion not completed and error is {e}')
    # for email in email_list:
    #       tnt_code=pd.read_sql(f"select tnt_code from dlc_tenant where email={email}",engine=conn)
def master_reg():
      try:
        for sheet in xl.sheet_names:
              sheet_name=sheet
              #ACADEMIC
              if sheet_name=='dlc_academic':
               academic=pd.read_excel(master_path,sheet_name='dlc_academic')
               academic.where(pd.notna(academic),None,inplace=True)

               for row in academic.itertuples(index=False):
                    print(row)
                    print(row[1])
                    email=f"'{row[1]}'"

                    print(f"email:'{email}'")
                    
                    cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
                    tnt_code=cursor.fetchone()[0]
                    print(tnt_code)
                    if  tnt_code:
                     print('starting insertion into dlc_academic')
                     cursor.execute(f"""
                       insert into dlc_academic
                     (academic_name,tenant_code,academic_startdate,academic_enddate,academic_description)
                       select %s,%s,%s,%s,%s
                      where not exists(select 1 from dlc_academic where academic_name=%s and 
                             tenant_code=%s and academic_startdate=%s and 
                                    academic_enddate=%s and academic_description=%s)  returning * """,
                       (row.academic_name,tnt_code,
                       row.academic_startdate,
                       row.academic_enddate,row.academic_description,row.academic_name,tnt_code,
                       row.academic_startdate,
                       row.academic_enddate,row.academic_description))
                     cnt=cursor.fetchall()
                     print(len(cnt))
                     print('Insertion of dlc_academic has been successful')
              #dlc_course_category
              if sheet_name=='dlc_course_category':
               course_cat=pd.read_excel(master_path,sheet_name='dlc_course_category')
               course_cat.where(pd.notna(course_cat),None,inplace=True)
               print(f'course_cat:{course_cat}')
               for row in course_cat.itertuples(index=False):
                    print(row)
                    print(row.tnt_code)
                    email=f"'{row.tnt_code}'"

                    print(f"email:'{email}'")
                    
                    cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
                    tnt_code=cursor.fetchone()[0]
                    print(tnt_code)
                    if  tnt_code:
                     print('starting insertion into dlc_course_category')
                     cursor.execute(f"""
                       insert into dlc_course_category
                     (course_category_name,course_category_description,tnt_code,course_category_status,image_url,course_category_moodleid,course_category_createdby)
                       select %s,%s,%s,%s,nullif(%s,'NaN'),%s,%s
                      where not exists(select 1 from dlc_course_category where course_category_name=%s and 
                             course_category_description=%s and tnt_code=%s and 
                                    course_category_status=%s and course_category_moodleid=%s)  returning * """,
                       (row.course_category_name,row.course_category_description,
                       tnt_code,
                       row.course_category_status,row.image_url,row.course_category_moodleid,1,
                     # where statement
                      row.course_category_name,row.course_category_description,
                       tnt_code,
                       row.course_category_status,row.course_category_moodleid))
                     cnt=cursor.fetchall()
                     print(len(cnt))
                     print('Insertion of dlc_course_category has been succesful')
        print('Insertion of master completed')
      except Exception as e:
       print(f'master reg not completed properly and the error is: {e}')

institue_reg()
master_reg()
conn.commit()
cursor.close()
conn.close()
exit()
