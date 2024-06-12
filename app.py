import http.client
import json
import requests
import time

# Function to fetch job data from LinkedIn Jobs API
def fetch_job_data():
    conn = http.client.HTTPSConnection("rapid-linkedin-jobs-api.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': "RAPID_API_KEY",
        'x-rapidapi-host': "rapid-linkedin-jobs-api.p.rapidapi.com"
    }
    conn.request("GET", "/search-jobs-v2?keywords=Developer&locationId=91000011&datePosted=past24Hours&onsiteRemote=remote&sort=mostRecent", headers=headers)
    res = conn.getresponse()
    data = res.read()
    json_data = json.loads(data.decode("utf-8"))
    return json_data

# Function to send message to Telegram group
def send_to_telegram(message):
    telegram_api_key = "TELEGRAM API KEY"
    telegram_group_id = "TELEGRAM GROUP ID"  # Replace with your group ID
    url = f"https://api.telegram.org/bot{telegram_api_key}/sendMessage"
    payload = {
        'chat_id': telegram_group_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    response = requests.post(url, data=payload)

# Function to split message into chunks
def split_message(message, max_length=4096):
    parts = []
    while len(message) > max_length:
        split_index = message.rfind("\n\n", 0, max_length)
        if split_index == -1:
            split_index = max_length
        parts.append(message[:split_index])
        message = message[split_index:]
    parts.append(message)
    return parts

# Function to process and send job data
def process_and_send_jobs():
    job_data = fetch_job_data()
    
    if 'data' in job_data:
        jobs = job_data['data']
        message_parts = []
        seen_job_ids = set()
        
        for job in jobs:
            job_id = job.get('id')
            if job_id in seen_job_ids:
                continue
            
            seen_job_ids.add(job_id)
            title = job.get('title', 'No Title')
            url = job.get('url', 'No URL')
            company = job.get('company', {})
            company_name = company.get('name', 'No Company Name')
            company_url = company.get('url', '')
            location = job.get('location', 'No Location')
            job_type = job.get('type', 'No Job Type')
            post_date = job.get('postDate', 'No Post Date')
            
            message_parts.append(
                f"*{title}*\n"
                f"Company: [{company_name}]({company_url})\n"
                f"Location: {location}\n"
                f"Type: {job_type}\n"
                f"Posted: {post_date}\n"
                f"[More Details]({url})"
            )
        
        # Combine all job messages into one
        full_message = "\n\n".join(message_parts)
        
        # Split the message if it exceeds the maximum length
        messages = split_message(full_message)
        
        # Send each message part to Telegram
        for message in messages:
            send_to_telegram(message)
            time.sleep(1)  # Sleep for 1 second between requests to avoid rate limiting
    else:
        print("Key 'data' not found in the JSON response")

# Cloud Function entry point
def main(request):
    process_and_send_jobs()
    return "Job postings sent to Telegram!", 200
