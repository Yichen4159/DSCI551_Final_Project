import os
import json


json_folder_path = './database/'  # JSON path
index_folder_path = './index/'  # Index folder path
index_file_path = './index/index.json'
# index path


def create_index(json_folder, index_folder):
    """
    Create an index for the JSON files stored in the given folder.
    The index will map each INCIDENT_NUMBER to its corresponding file path.
    """
    index = {}
    index_file = os.path.join(index_folder, 'index.json')

    # Ensure index folder exists
    os.makedirs(index_folder, exist_ok=True)

    # Iterate over all JSON files in the folder
    for file_name in os.listdir(json_folder):
        if file_name.endswith('.json'):
            incident_number = file_name.split('.')[0]
            index[incident_number] = os.path.join(json_folder, file_name)

    # Save the index to a JSON file
    with open(index_file, 'w') as file:
        json.dump(index, file)

    return index_file


def add_to_index(index_file, incident_number, file_path):
    """
    Add a new entry to the index.
    """
    # Load the current index
    with open(index_file, 'r') as file:
        index = json.load(file)

    # Add the new entry
    index[incident_number] = file_path

    # Save the updated index
    with open(index_file, 'w') as file:
        json.dump(index, file)


def remove_from_index(index_file, incident_number):
    """
    Remove an entry from the index.
    """
    # Load the current index
    with open(index_file, 'r') as file:
        index = json.load(file)

    # Remove the entry
    if incident_number in index:
        del index[incident_number]

    # Save the updated index
    with open(index_file, 'w') as file:
        json.dump(index, file)


create_index(json_folder_path, index_folder_path)


# demo for adding or deleting json
# add_to_index(index_file_path, 'new_incident_number', 'path/to/new/file')
# remove_from_index(index_file_path, 'I152049924')
