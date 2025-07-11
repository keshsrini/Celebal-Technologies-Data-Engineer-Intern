def process_ecom_order(file):
    df = pd.read_csv(file.path)
    truncate_and_load('H_ECOM_Orders', df)
