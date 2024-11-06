from flask import Flask, request, jsonify
import ftplib
import json
import os
import logging
from datetime import datetime

# Configuration
FTP_HOST = '37.60.247.110'
FTP_USER = 'azpftp'
FTP_PASS = 'bashajama'
FTP_FILE_PATH = '/gds/gps.json'
LOCAL_FILE = 'local_data.json'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)


def upload_json_to_ftp(json_data):
    """Upload JSON data to the FTP server."""
    temp_file_path = 'temp_file.json'
    try:
        # Log the data that will be sent to FTP
        logging.info("Preparing to upload the following data to FTP:")
        logging.info(json.dumps(json_data, indent=4))

        # Save new JSON data to a temporary file and upload
        with open(temp_file_path, 'w') as temp_file:
            json.dump(json_data, temp_file, indent=4)

        with ftplib.FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd('/gds')

            with open(temp_file_path, 'rb') as temp_file:
                ftp.storbinary(f"STOR {FTP_FILE_PATH}", temp_file)
            logging.info("JSON file updated on FTP server.")

        # Remove the temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logging.info("Temporary JSON file deleted.")

    except ftplib.all_errors as e:
        logging.error(f"FTP error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during FTP upload: {e}")


@app.route('/upload', methods=['POST'])
def upload_data():
    """Endpoint to receive and process JSON data."""
    try:
        # Extract JSON data from the request
        if request.is_json:
            request_data = request.get_json()

            # Extract incoming fields
            incoming_data = request_data.get('data')
            if not incoming_data:
                return jsonify({"error": "Missing data field."}), 400

            if not isinstance(incoming_data, list):  # Expecting a list of JSON objects
                return jsonify({"error": "Invalid data format. Expected a list of JSON objects."}), 400

            # Initialize variables
            existing_data = {}
            id_counter = 1  # Starting ID counter

            # Load existing data if available
            if os.path.exists(LOCAL_FILE):
                with open(LOCAL_FILE, 'r') as f:
                    existing_data_list = json.load(f)
                    # Convert list to dictionary by id
                    for entry in existing_data_list:
                        existing_data[entry['id']] = entry

            # Process incoming data
            for item in incoming_data:
                try:
                    data_id = item.get('ID')
                    vessel_id = item.get('VesselNumber')
                    data = item.get('Data')
                    if not data_id or not vessel_id:
                        return jsonify({"error": "Missing ID or VesselNumber in the data."}), 400

                    # Convert the timestamp placeholder to current time if not provided
                    timestamp = datetime.now().isoformat()

                    # Prepare the GPS data item
                    gps_item = {
                        "data_id": data_id,
                        "nmea": data,
                        "timestamp": timestamp
                    }

                    # Check if the vessel_id is already in the existing_data
                    found = False
                    for entry in existing_data.values():
                        if entry["vessel_id"] == vessel_id:
                            entry["gps"].append(gps_item)
                            found = True
                            break

                    if not found:
                        new_id = str(id_counter)
                        id_counter += 1
                        existing_data[new_id] = {
                            "id": new_id,
                            "vessel_id": vessel_id,
                            "gps": [gps_item]
                        }

                except KeyError as e:
                    logging.error(f"Missing key in GPS data: {e}")
                    return jsonify({"error": "Missing key in GPS data."}), 400

            # Convert combined_data dictionary to list
            final_data = list(existing_data.values())

            # Save data locally
            with open(LOCAL_FILE, 'w') as f:
                json.dump(final_data, f, indent=4)
            logging.info("Data saved locally.")

            # Upload data to FTP
            upload_json_to_ftp(final_data)

            return jsonify({"message": "Data received and processed successfully."}), 200

        else:
            return jsonify({"error": "Request must be JSON."}), 400

    except Exception as e:
        logging.error(f"Error in /upload endpoint: {e}")
        return jsonify({"error": "Internal server error."}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=2024, debug=True)
