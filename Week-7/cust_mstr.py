def process_cust_mstr(file, date_str):
    file_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    df = pd.read_csv(file.path)
    df['FileDate'] = file_date
    
    truncate_and_load('CUST_MSTR', df)
