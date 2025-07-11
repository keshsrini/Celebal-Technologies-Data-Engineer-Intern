def truncate_and_load(table_name, dataframe):
    
    conn = create_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
    dataframe.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
