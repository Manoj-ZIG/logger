import requests
 
 
def send_payload(api_endpoint_url, file_path, job_type, 
                 comments, auth_tok, payload='', added_by='', notify_to='', 
                 should_notify='', job_status='pending'):
    api_url = api_endpoint_url
    token = auth_tok
    return_payload = {
        "filePath": file_path,
        "job_type": job_type,
        "job_status": job_status,
        "payload": payload,
        "comments": comments,
        "added_by": added_by,
        "notify_to": notify_to,
        "should_notify": should_notify
    }
    response = requests.post(api_url, json=return_payload, headers = {"Authorization":f"Bearer {token}"})
    print(f'status_code: {response.status_code} |{response.json()}')
 
    return response.status_code