import mailbox

def process_mbox_chunk(input_file, chunk_size=1000):
    mbox = mailbox.mbox(input_file)
    count = 0
    emails = []

    for i, message in enumerate(mbox):
        # Extract data from the message (e.g., subject, from, date, body)
        email_data = {
            "subject": message['subject'],
            "from": message['from'],
            "date": message['date'],
            "body": message.get_payload(decode=True).decode(errors='ignore')
        }
        emails.append(email_data)
        count += 1

        if count >= chunk_size:
            yield emails
            emails = []
            count = 0

    if emails:
        yield emails

# Example usage:
input_file = 'input.mbox'
chunk_size = 1000  # Number of emails to process in each chunk

for chunk_number, email_chunk in enumerate(process_mbox_chunk(input_file, chunk_size), start=1):
    print(f"Processing chunk {chunk_number} with {len(email_chunk)} emails")
    # Here you can process the extracted data from email_chunk
    # For example, save to a database, write to a file, or further process each email

    # Example: Printing subject of the first email in the chunk
    if email_chunk:
        print("First email subject in this chunk:", email_chunk[0]['subject'])
