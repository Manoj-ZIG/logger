

def save_data_loss_log(s3_c, bucket_name, key, data_loss_str, if_first_data=False):
    """ 
    Function use to save the data loss at each step
    args:
        s3_c: object s3 client
        bucket_name: str bucket name (where the txt log file get stored)
        key: str s3 file path key
        data_loss_str: str (input string to append in txt file)
        if_first_data: bool (as S3 don't have append functionality so this bool will handle a operation)
    return:
        None (put the txt file to s3)
    
    data_loss_str e.g. --> "raw data table | (50,3) | 0%" 
    """
    def calculate_loss(prev_loss_str, curr_loss_str):
        """
        return the percentage loss w.r.t on original raw data
        """
        prev_shape = eval(prev_loss_str.split("|")[1])[0]
        curr_shape = eval(curr_loss_str.split("|")[1])[0]
        try:
            percentage_loss = (int(prev_shape) -
                               int(curr_shape))/int(prev_shape)
        except Exception as e:
            percentage_loss = 0
        return percentage_loss

    # get current data
    if not if_first_data:
        curr_data = s3_c.get_object(Bucket=bucket_name,
                                    Key=key)
        curr_data_str = curr_data['Body'].read().decode('utf-8')

        # dump the data
        loss = calculate_loss(curr_data_str, data_loss_str)
        data_loss_str = f"{data_loss_str} | {loss}%\n"
        new_data = curr_data_str + data_loss_str
        s3_c.put_object(Bucket=bucket_name, Body=str(new_data),
                        Key=key)
    else:
        data_loss_str = f"{data_loss_str}\n"
        s3_c.put_object(Bucket=bucket_name, Body=str(data_loss_str),
                        Key=key)
