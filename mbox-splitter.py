import mailbox
import os

def split_mbox(input_file, output_dir, chunk_size):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    mbox = mailbox.mbox(input_file)
    chunk = 1
    count = 0
    new_mbox = None

    for i, message in enumerate(mbox):
        if count == 0:
            chunk_filename = os.path.join(output_dir, f'chunk_{chunk}.mbox')
            new_mbox = mailbox.mbox(chunk_filename)
            print(f"Creating {chunk_filename}")

        new_mbox.add(message)
        count += 1

        if count >= chunk_size:
            new_mbox.close()
            count = 0
            chunk += 1

    if new_mbox:
        new_mbox.close()

    print(f"Splitting completed. {chunk} chunks created.")

# Example usage:
input_file = 'input.mbox'
output_dir = 'out'
chunk_size = 2000  # Number of emails per chunk

split_mbox(input_file, output_dir, chunk_size)
