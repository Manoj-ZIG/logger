import re

class ReceivedFileLogs:
    
    def __init__(self, s3r, client_name, received_file_logs_bkt, received_file_logs_path, received_file_logs_key):
        self.s3r = s3r
        self.client_name = client_name
        self.received_file_logs_bkt = received_file_logs_bkt
        self.received_file_logs_path = received_file_logs_path
        self.received_file_logs_key = received_file_logs_key

    def update_received_file_logs(self, act_file, load_date, category, size, zigna_file_name):
        '''
        ### Update the Received file logs
        
        args:
            act_file: The actual file name
            load_date: Date when we received it
            category: Type of the document
            size: Size of the document
            zigna_file_name: cleaned file name
        return:
            None
        '''
        print("Reading the received file logs")
        status = f"{category}_received"
        content = f"{act_file}|{load_date}|{status}|{size}|{zigna_file_name}\n"
        
        received_file_logs_file_name = self.received_file_logs_path + zigna_file_name.split(".")[0] + ".txt"
        print("Received file logs key : ", received_file_logs_file_name)
        try:
            response = self.s3r.Bucket(self.received_file_logs_bkt).Object(received_file_logs_file_name).get()
            existing_content = response['Body'].read().decode('utf-8')
        except self.s3r.meta.client.exceptions.NoSuchKey:
            existing_content = "FILENAME|RECEIVED_DATE|STATUS|SIZE|ZIGNA_FILE_NAME\n"
            
        existing_content += content 
        self.s3r.Bucket(self.received_file_logs_bkt).Object(received_file_logs_file_name).put(Body=existing_content.encode('utf-8'))
        print(f"Updated the received file logs for '{act_file}' | zigna_file_name - '{zigna_file_name}'")