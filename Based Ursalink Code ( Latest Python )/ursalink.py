import socket
import json
import requests
import os
import time
import random
import threading
import queue

# Configuration
HOST = '192.168.1.1'
PORT = 502
INTERNET_TEST_URL = 'http://www.google.com'
LOCAL_FILE = 'tmp/local_vessel.json'
UPLOAD_URL = 'http://37.60.247.110:2024/upload'  # Replace with your actual endpoint
VESSEL_NUMBER = '14'
data_queue = queue.Queue()  # Queue to handle incoming data
lock = threading.Lock()  # Lock to handle safe access to local data
internet_connected = True  # Track internet connection status


def check_internet():
    """Check if internet connection is available."""
    global internet_connected
    try:
        response = requests.get(INTERNET_TEST_URL, timeout=15)
        if response.status_code == 200:
            internet_connected = True
            return True
    except requests.RequestException:
        internet_connected = False
    return False


def generate_unique_id():
    """Generate a unique ID in the format AZP-{randomnumber}."""
    random_number = random.randint(1000, 9999)
    return f"AZP-{random_number}"


def save_data_locally(data):
    """Save data from the queue to a local JSON file."""
    with lock:
        if os.path.exists(LOCAL_FILE):
            with open(LOCAL_FILE, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        for entry in data:
            unique_id = generate_unique_id()  # Generate unique ID for each entry
            entry_data = {
                'VesselNumber': VESSEL_NUMBER,
                'UniqueID': unique_id,
                'RawData': entry
            }
            existing_data.append(entry_data)

            # Log the data being saved
            print(f"Saving to local file: {entry_data}")

        with open(LOCAL_FILE, 'w') as f:
            json.dump(existing_data, f, indent=4)


def upload_data():
    """Upload data from the local file to the server."""
    while True:
        if os.path.exists(LOCAL_FILE):
            try:
                with lock:
                    with open(LOCAL_FILE, 'r') as f:
                        data = json.load(f)

                if not data:
                    print("No data to upload.")
                    time.sleep(10)  # Check for upload every 10 seconds
                    continue

                payload = {
                    "VesselNumber": VESSEL_NUMBER,
                    "data": data
                }

                # Log the data being sent
                print(f"Uploading data to server: {json.dumps(payload, indent=4)}")

                response = requests.post(UPLOAD_URL, json=payload)

                if response.status_code == 200:
                    print("Data successfully uploaded.")
                    with lock:
                        os.remove(LOCAL_FILE)  # Remove the local file after successful upload
                else:
                    print(f"Failed to upload data. Status code: {response.status_code}")
                    print(f"Response content: {response.text}")

            except (requests.RequestException, IOError, json.JSONDecodeError) as e:
                print(f"Error during upload: {e}")

        time.sleep(10)  # Check for upload every 10 seconds


def process_data(data):
    """Process the received data as a string and filter only data starting with '$'."""
    try:
        decoded_data = data.decode('ascii', errors='ignore')
        cleaned_data = decoded_data.strip().replace('\r', '').replace('\n', '')

        # Log the raw data received from the socket
        print(f"Raw data received: {cleaned_data}")

        if cleaned_data.startswith('$'):
            return [cleaned_data]
        else:
            return []
    except Exception as e:
        print(f"Error processing data: {e}")
        return []


def receive_data():
    """Receive data from TCP and save to queue."""
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))
                print(f"Connected to {HOST}:{PORT}")

                while True:
                    try:
                        data = s.recv(1024)
                        if not data:
                            print("No data received or connection closed.")
                            break

                        string_data_list = process_data(data)
                        if string_data_list:
                            data_queue.put(string_data_list)  # Add data to the queue
                            print(f"Data added to queue: {string_data_list}")

                    except ConnectionResetError:
                        print("Connection was forcibly closed by the remote host. Reconnecting...")
                        break

        except (socket.error, ConnectionResetError) as e:
            print(f"Connection error: {e}. Reconnecting in 60 seconds...")
            time.sleep(60)  # Reconnect after a delay


def monitor_internet():
    """Periodically check internet connectivity."""
    while True:
        if not check_internet():
            print("Internet is not available.")
        else:
            print("Internet is available.")
        time.sleep(3600)  # Check every hour


def main():
    """Main function that runs the receive and upload threads."""

    # Thread to save data from queue locally
    def queue_to_local():
        while True:
            if not data_queue.empty():
                data_list = data_queue.get()
                save_data_locally(data_list)
            time.sleep(1)

    # Start the TCP receive thread
    receiver_thread = threading.Thread(target=receive_data)
    receiver_thread.daemon = True
    receiver_thread.start()

    # Start the local save thread
    save_thread = threading.Thread(target=queue_to_local)
    save_thread.daemon = True
    save_thread.start()

    # Start the upload thread
    uploader_thread = threading.Thread(target=upload_data)
    uploader_thread.daemon = True
    uploader_thread.start()

    # Start the internet monitoring thread
    internet_thread = threading.Thread(target=monitor_internet)
    internet_thread.daemon = True
    internet_thread.start()

    # Keep the main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
