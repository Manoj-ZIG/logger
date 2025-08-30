import requests


def send_payload(api_endpoint_url, file_path, access_token, mr_name, job_type, comments, payload='', added_by='', notify_to='', should_notify='', job_status='pending'):
    api_url = api_endpoint_url
    return_payload = {
        "filePath": f"/{file_path}",
        "job_type": job_type,
        "job_status": job_status,
        "payload": payload,
        "comments": comments,
        "added_by": added_by,
        "notify_to": notify_to,
        "should_notify": should_notify,
        "medical_record_name": mr_name
    }
    # response = requests.post(api_url, json=return_payload)
    response = requests.post(api_endpoint_url,
      headers={'Authorization': 'Bearer {}'.format(access_token)}, json=return_payload)
    print(f'status_code: {response.status_code} |{response.json()}')

    return response.status_code
