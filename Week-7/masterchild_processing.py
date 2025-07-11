def process_master_child(file, date_str):
    file_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    date_key = date_str  
  
    df = pd.read_csv(file.path)
    
    df['FileDate'] = file_date
    df['DateKey'] = date_key
    truncate_and_load('master_child', df)
