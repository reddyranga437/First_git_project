# import pandas as pd 
# import psycopg2 
# from sqlalchemy import engine
# master_path=r'D:\practice\Kemis_masters.xlsx'
# path=r'D:\practice\institute.csv'
# fun_path=r'D:\practice\institute'
# df=pd.read_csv(path)
# df.where(pd.notna(df),None,inplace=True)
# print(df)
# xl=pd.ExcelFile(master_path)
# print(xl)
# #engine=engine.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/demo')
# email_list=df.email.to_list()
# conn=psycopg2.connect('dbname=demo user=postgres password=postgres')
# cursor=conn.cursor()
# print(email_list)
# def institue_reg():
#   try:
#           for index,row in df.iterrows():
#                 email=row['email']
#                 print(email)
#                 cursor.execute('select distinct email from dlc_tenant where email=%s',(email,))
#                 result=cursor.fetchone()
#                 print(result)
#                 if  result is  None:
#                    cursor.execute(f"""
#                    call sp_upload_institutions('{fun_path}')
#                    """)
#                    print('Institute inserted')
#                 else:
#                     print('Institute already exists')
#           print('Institution complete')
#   except Exception as e:
#      print(f'Institute insertion not completed and error is {e}')
#     # for email in email_list:
#     #       tnt_code=pd.read_sql(f"select tnt_code from dlc_tenant where email={email}",engine=conn)
# def master_reg():
#       try:
#         for sheet in xl.sheet_names:
#               sheet_name=sheet
#               #ACADEMIC
#               if sheet_name=='dlc_academic':
#                academic=pd.read_excel(master_path,sheet_name='dlc_academic')
#                academic.where(pd.notna(academic),None,inplace=True)

#                for row in academic.itertuples(index=False):
#                     print(row)
#                     print(row[1])
#                     email=f"'{row[1]}'"

#                     print(f"email:'{email}'")
                    
#                     cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
#                     tnt_code=cursor.fetchone()[0]
#                     print(tnt_code)
#                     if  tnt_code:
#                      print('starting insertion into dlc_academic')
#                      cursor.execute(f"""
#                        insert into dlc_academic
#                      (academic_name,tenant_code,academic_startdate,academic_enddate,academic_description)
#                        select %s,%s,%s,%s,%s
#                       where not exists(select 1 from dlc_academic where academic_name=%s and 
#                              tenant_code=%s and academic_startdate=%s and 
#                                     academic_enddate=%s and academic_description=%s)  returning * """,
#                        (row.academic_name,tnt_code,
#                        row.academic_startdate,
#                        row.academic_enddate,row.academic_description,row.academic_name,tnt_code,
#                        row.academic_startdate,
#                        row.academic_enddate,row.academic_description))
#                      cnt=cursor.fetchall()
#                      print(len(cnt))
#                      print('Insertion of dlc_academic has been successful')
#               #dlc_course_category
#               if sheet_name=='dlc_course_category':
#                course_cat=pd.read_excel(master_path,sheet_name='dlc_course_category')
#                course_cat.where(pd.notna(course_cat),None,inplace=True)
#                print(f'course_cat:{course_cat}')
#                for row in course_cat.itertuples(index=False):
#                     print(row)
#                     print(row.tnt_code)
#                     email=f"'{row.tnt_code}'"

#                     print(f"email:'{email}'")
                    
#                     cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
#                     tnt_code=cursor.fetchone()[0]
#                     print(tnt_code)
#                     if  tnt_code:
#                      print('starting insertion into dlc_course_category')
#                      cursor.execute(f"""
#                        insert into dlc_course_category
#                      (course_category_name,course_category_description,tnt_code,course_category_status,image_url,course_category_moodleid,course_category_createdby)
#                        select %s,%s,%s,%s,nullif(%s,'NaN'),%s,%s
#                       where not exists(select 1 from dlc_course_category where course_category_name=%s and 
#                              course_category_description=%s and tnt_code=%s and 
#                                     course_category_status=%s and course_category_moodleid=%s)  returning * """,
#                        (row.course_category_name,row.course_category_description,
#                        tnt_code,
#                        row.course_category_status,row.image_url,row.course_category_moodleid,1,
#                      # where statement
#                       row.course_category_name,row.course_category_description,
#                        tnt_code,
#                        row.course_category_status,row.course_category_moodleid))
#                      cnt=cursor.fetchall()
#                      print(len(cnt))
#                      print('Insertion of dlc_course_category has been succesful')
#                #dlc_course
#               if sheet_name=='dlc_course':
#                course=pd.read_excel(master_path,sheet_name='dlc_course')
#                course.where(pd.notna(course),None,inplace=True)
#                print(f'course:{course}')
#                for row in course.itertuples(index=False):
#                     print(row)
#                     print(row.tnt_code)
#                     email=f"'{row.tnt_code}'"
#                     course_cat=f"'{row.course_category_id}'"

#                     print(f"email:'{email}'")
                    
#                     cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
#                     tnt_code=cursor.fetchone()[0]
#                     cursor.execute(f'select course_category_id from dlc_course_category where trim(lower(course_category_name))=trim(lower({course_cat}))')
#                     course_cat_id=cursor.fetchone()[0]
#                     print(tnt_code)
          
#                     if  tnt_code:
#                      print('starting insertion into dlc_course')
#                      cursor.execute(f"""
#                        insert into dlc_course
#                      (course_name,course_description,course_fees,course_duration,tnt_code,
#                                     course_category_id,course_designed_for,
#                                     coursetime_duration,thereticalpercentage)
#                        select %s,%s,%s,%s,%s,%s,%s,%s,%s
#                       where not exists(select 1 from dlc_course where course_name=%s and 
#                              course_description=%s and course_fees=%s and course_duration=%s
#                                      and  tnt_code=%s and course_category_id=%s and
#                                     course_designed_for=%s and coursetime_duration=%s and thereticalpercentage=%s)  returning * """,
#                        (row.course_name,row.course_description,row.course_fees,row.course_duration,
#                        tnt_code,
#                        course_cat_id,row.course_designed_for,row.coursetime_duration,row.thereticalpercentage,
#                      # where statement
#                       row.course_name,row.course_description,row.course_fees,row.course_duration,
#                        tnt_code,
#                        course_cat_id,row.course_designed_for,row.coursetime_duration,row.thereticalpercentage))
#                      cnt=cursor.fetchall()
#                      print(len(cnt))
#                      print('Insertion of dlc_course has been succesful')
#                #dlc_course
#               if sheet_name=='dlc_course':
#                course=pd.read_excel(master_path,sheet_name='dlc_course')
#                course.where(pd.notna(course),None,inplace=True)
#                print(f'course:{course}')
#                for row in course.itertuples(index=False):
#                     print(row)
#                     print(row.tnt_code)
#                     email=f"'{row.tnt_code}'"
#                     course_cat=f"'{row.course_category_id}'"

#                     print(f"email:'{email}'")
                    
#                     cursor.execute(f'select tnt_code from dlc_tenant where email={email} and tnt_status=true limit 1')
#                     tnt_code=cursor.fetchone()[0]
#                     cursor.execute(f'select course_category_id from dlc_course_category where trim(lower(course_category_name))=trim(lower({course_cat}))')
#                     course_cat_id=cursor.fetchone()[0]
#                     print(tnt_code)
          
#                     if  tnt_code:
#                      print('starting insertion into dlc_course')
#                      cursor.execute(f"""
#                        insert into dlc_course
#                      (course_name,course_description,course_fees,course_duration,tnt_code,
#                                     course_category_id,course_designed_for,
#                                     coursetime_duration,thereticalpercentage)
#                        select %s,%s,%s,%s,%s,%s,%s,%s,%s
#                       where not exists(select 1 from dlc_course where course_name=%s and 
#                              course_description=%s and course_fees=%s and course_duration=%s
#                                      and  tnt_code=%s and course_category_id=%s and
#                                     course_designed_for=%s and coursetime_duration=%s and thereticalpercentage=%s)  returning * """,
#                        (row.course_name,row.course_description,row.course_fees,row.course_duration,
#                        tnt_code,
#                        course_cat_id,row.course_designed_for,row.coursetime_duration,row.thereticalpercentage,
#                      # where statement
#                       row.course_name,row.course_description,row.course_fees,row.course_duration,
#                        tnt_code,
#                        course_cat_id,row.course_designed_for,row.coursetime_duration,row.thereticalpercentage))
#                      cnt=cursor.fetchall()
#                      print(len(cnt))
#                      print('Insertion of dlc_course has been succesful')
#         print('Insertion of master completed')
#       except Exception as e:
#        print(f'master reg not completed properly and the error is: {e}')

# institue_reg()
# master_reg()
# conn.commit()
# cursor.close()
# conn.close()
# exit()


import pandas as pd
import psycopg2

# File paths
MASTER_PATH = r'D:\practice\Kemis_masters.xlsx'
CSV_PATH = r'D:\practice\institute.csv'
FUN_PATH = r'D:\practice\institute'

# Read CSV data
df = pd.read_csv(CSV_PATH)
df.where(pd.notna(df), None, inplace=True)
email_list = df['email'].tolist()

def connect_db():
    return psycopg2.connect(dbname='demo', user='postgres', password='postgres')

def institue_reg(cursor):
    try:
        for _, row in df.iterrows():
            email = row['email']
            cursor.execute('SELECT 1 FROM dlc_tenant WHERE email = %s LIMIT 1', (email,))
            result = cursor.fetchone()
            if result is None:
                cursor.execute("CALL sp_upload_institutions(%s)", (FUN_PATH,))
                print(f'Inserted new institute for email: {email}')
            else:
                print(f'Institute already exists for email: {email}')
        print('Institution registration complete')
    except Exception as e:
        print(f'Institution registration failed: {e}')

def get_tenant_code(cursor, email):
    cursor.execute(
        "SELECT tnt_code FROM dlc_tenant WHERE email = %s AND tnt_status = TRUE LIMIT 1",
        (email,)
    )
    result = cursor.fetchone()
    return result[0] if result else None

def get_course_category_id(cursor, name):
    cursor.execute(
        "SELECT course_category_id FROM dlc_course_category WHERE TRIM(LOWER(course_category_name)) = TRIM(LOWER(%s))",
        (name,)
    )
    result = cursor.fetchone()
    return result[0] if result else None

def get_academic_year(cursor, name):
    cursor.execute(
        "SELECT academic_id FROM dlc_academic WHERE TRIM(LOWER(academic_name)) = TRIM(LOWER(%s))",
        (name,)
    )
    result = cursor.fetchone()
    return result[0] if result else None

def insert_academic(cursor, academic_df):
    for row in academic_df.itertuples(index=False):

        tnt_code = get_tenant_code(cursor, row.tenant_code)
        if not tnt_code:
            continue
        cursor.execute("""
            INSERT INTO dlc_academic (
                academic_name, tenant_code, academic_startdate, academic_enddate, academic_description
            )
            SELECT %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dlc_academic WHERE academic_name=%s AND tenant_code=%s AND
                academic_startdate=%s AND academic_enddate=%s AND academic_description=%s
            )
            RETURNING *
        """, (
            row.academic_name, tnt_code, row.academic_startdate, row.academic_enddate, row.academic_description,
            row.academic_name, tnt_code, row.academic_startdate, row.academic_enddate, row.academic_description
        ))
        inserted = cursor.fetchall()
        print(f'Inserted {len(inserted)} records into dlc_academic')

def insert_course_category(cursor, category_df):
    for row in category_df.itertuples(index=False):
        tnt_code = get_tenant_code(cursor, row.tnt_code)
        if not tnt_code:
            continue
        cursor.execute("""
            INSERT INTO dlc_course_category (
                course_category_name, course_category_description, tnt_code,
                course_category_status, image_url, course_category_moodleid, course_category_createdby
            )
            SELECT %s, %s, %s, %s, NULLIF(%s, 'NaN'), %s, 1
            WHERE NOT EXISTS (
                SELECT 1 FROM dlc_course_category WHERE
                course_category_name=%s AND course_category_description=%s AND tnt_code=%s AND
                course_category_status=%s AND course_category_moodleid=%s
            )
            RETURNING *
        """, (
            row.course_category_name, row.course_category_description, tnt_code,
            row.course_category_status, row.image_url, row.course_category_moodleid,
            row.course_category_name, row.course_category_description, tnt_code,
            row.course_category_status, row.course_category_moodleid
        ))
        inserted = cursor.fetchall()
        print(f'Inserted {len(inserted)} records into dlc_course_category')

def insert_course(cursor, course_df):
    for row in course_df.itertuples(index=False):
        tnt_code = get_tenant_code(cursor, row.tnt_code)
        course_cat_id = get_course_category_id(cursor, row.course_category_id)
        if not tnt_code or not course_cat_id:
            continue
        cursor.execute("""
            INSERT INTO dlc_course (
                course_name, course_description, course_fees, course_duration, tnt_code,
                course_category_id, course_designed_for, coursetime_duration, thereticalpercentage
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dlc_course WHERE course_name=%s AND course_description=%s AND
                course_fees=%s AND course_duration=%s AND tnt_code=%s AND course_category_id=%s AND
                course_designed_for=%s AND coursetime_duration=%s AND thereticalpercentage=%s
            )
            RETURNING *
        """, (
            row.course_name, row.course_description, row.course_fees, row.course_duration,
            tnt_code, course_cat_id, row.course_designed_for, row.coursetime_duration, row.thereticalpercentage,
            row.course_name, row.course_description, row.course_fees, row.course_duration,
            tnt_code, course_cat_id, row.course_designed_for, row.coursetime_duration, row.thereticalpercentage
        ))
        inserted = cursor.fetchall()
        print(f'Inserted {len(inserted)} records into dlc_course')


def insert_dlc_cohort_management(cursor, course_df):
    for row in course_df.itertuples(index=False):
        tnt_code = get_tenant_code(cursor, row.tnt_code)
        academic_id = get_academic_year(cursor, row.academic_year_id)
       
        if not tnt_code:
            continue
        cursor.execute("""
            INSERT INTO dlc_cohort_management (
                cohort_name,cohort_code,cohort_description,academic_year_id,commencement_date,
                cohort_period,application_startdate,application_enddate,
                tnt_code,intake_month,intake_year,cohortcode_number

            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s
            WHERE NOT EXISTS (
                SELECT 1 FROM dlc_cohort_management WHERE cohort_name=%s AND cohort_code=%s AND
                cohort_description=%s AND academic_year_id=%s AND commencement_date=%s 
                        AND
                cohort_period=%s AND application_startdate=%s AND application_enddate=%s
                and tnt_code=%s and intake_month=%s and intake_year=%s and cohortcode_number=%s
            )
            RETURNING *
        """, (
            row.cohort_name, row.cohort_code, row.cohort_description, academic_id,
            row.commencement_date,  row.cohort_period, 
            row.application_startdate, row.application_enddate,tnt_code,row.intake_month,
            row.intake_year,row.cohortcode_number,
            row.cohort_name, row.cohort_code, row.cohort_description, academic_id,
            row.commencement_date,  row.cohort_period, 
            row.application_startdate, row.application_enddate,tnt_code,row.intake_month,
            row.intake_year,row.cohortcode_number
        ))
        inserted = cursor.fetchall()
        print(f'Inserted {len(inserted)} records into dlc_cohort_management')

def master_reg(cursor):
    try:
        xl = pd.ExcelFile(MASTER_PATH)
        for sheet in xl.sheet_names:
            df_sheet = pd.read_excel(MASTER_PATH, sheet_name=sheet)
            df_sheet.where(pd.notna(df_sheet), None,inplace=True)
            

            if sheet == 'dlc_academic':
                insert_academic(cursor, df_sheet)
            elif sheet == 'dlc_course_category':
                insert_course_category(cursor, df_sheet)
            elif sheet == 'dlc_course':
                insert_course(cursor, df_sheet)
            elif sheet == 'dlc_cohort_management':
                insert_dlc_cohort_management(cursor, df_sheet)

        print('Master registration complete')
    except Exception as e:
        print(f'Master registration failed: {e}')

def main():
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                institue_reg(cursor)
                master_reg(cursor)
            conn.commit()
            print("All changes committed successfullY")
    except Exception as e:
        print(f'Failed to complete process: {e}')

if __name__ == "__main__":
    main()
