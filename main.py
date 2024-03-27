import pandas as pd
import os
import json
import index
from grammar import query_keywords
from query import projection, filtering, group_by, order_by, aggregate, count_unique
import random


# File paths
csv_file_path = 'crime.csv'
database_directory = './database/'
index_file_path = './index/index.json'  # index path


# Define the function to load data from the CSV file and save as JSON
def load_data(csv_path, database_dir, n=1000):
    # Read the CSV file with ISO-8859-1 encoding
    df = pd.read_csv(csv_path, encoding='ISO-8859-1')

    # Randomly select n rows from the dataframe
    df_sample = df.sample(n=n)
    print(df_sample.head(10))
    # Create the directory if it doesn't exist
    os.makedirs(database_dir, exist_ok=True)

    # Convert each row to JSON and save
    for _, row in df_sample.iterrows():
        incident_number = row['INCIDENT_NUMBER']
        json_path = os.path.join(database_dir, f"{incident_number}.json")
        row.to_json(json_path)

    print(f"{n} records have been successfully converted to JSON and saved in {database_dir}")


def insert(data_str, database_dir, index_file):
    # parse the input
    data_str = data_str.replace('insert ', '')  # Remove command part
    items = data_str.split(' ')
    data = {}
    key = None

    # iterate though input
    for item in items:
        if ':' in item:
            if key is not None:
                data[key] = value  # save last key-value pair
            key = item.replace(':', '')
            # print(key)
            value = ''
        else:
            value = item  # update value

    if key is not None:
        data[key] = value

    # create and save json
    incident_number = data['INCIDENT_NUMBER']
    json_path = os.path.join(database_dir, f"{incident_number}.json")
    with open(json_path, 'w') as file:
        json.dump(data, file)

    # update index
    index.add_to_index(index_file, incident_number, json_path)

    print(f"Record {incident_number} has been inserted and indexed.")


def delete(incident_number, database_dir, index_file):
    # create json path
    json_path = os.path.join(database_dir, f"{incident_number}.json")

    # check if number exist
    if os.path.exists(json_path):
        os.remove(json_path)
        print(f"Record {incident_number} has been deleted.")
    else:
        print(f"No record found for {incident_number}.")

    # delete index
    index.remove_from_index(index_file, incident_number)
    print(f"Record {incident_number} has been removed from the index.")


def update(incident_number, key, new_value, database_dir):
    # Build json file path
    json_path = os.path.join(database_dir, f"{incident_number}.json")

    # Check if path exists
    if os.path.exists(json_path):
        # Read in data
        with open(json_path, 'r') as file:
            data = json.load(file)

        # Update data
        data[key] = new_value

        # Save updated data
        with open(json_path, 'w') as file:
            json.dump(data, file)

        print(f"Record {incident_number} has been updated with {key}: {new_value}.")
    else:
        print(f"No record found for {incident_number}.")


def main():
    print("Welcome to BostonCrime DB!")
    while True:
        command: str = input("MyDB> ").strip()
        command = command.replace(' than', '')
        words = command.split()

        if 'order by' in command:
            order_by_index = command.lower().index('order by')
            before_order_by = command[:order_by_index].strip().split()
            after_order_by = command[order_by_index:].strip().split()
            words = before_order_by + ['order by'] + after_order_by[2:]

        # Exit command
        if command.lower() == "exit":
            break

        # Load demo data
        elif command.lower() == "demo":
            load_data(csv_file_path, database_directory)
            print("Demo data loaded!")

        elif command.lower().startswith("insert"):
            insert(command[7:], database_directory, index_file_path)

        elif command.lower().startswith("delete"):
            delete(command.split(' ')[1], database_directory, index_file_path)

        elif command.lower().startswith("update"):
            _, incident_number, key, new_value = command.split(' ')
            update(incident_number, key, new_value, database_directory)

        elif words[0].lower() in query_keywords['projection']:
            if len(words) == 2:  # INCIDENT_NUMBER
                result = projection(words[1], database_directory)
                print(result)
            elif words[2].lower() in query_keywords['filter']:
                feature_to_print = words[1]
                filter_feature = words[3]
                condition = words[4]
                value = words[5]
                result = filtering(feature_to_print, filter_feature, condition, value, database_directory)
                print(result)
            elif 'group by' in command:
                group_feature = words[-1]  # Extract group by words
                feature_to_print = words[1]  #
                result = group_by(group_feature, database_directory)
                # Print output
                for key, records in result.items():
                    print(f"{key}:")
                    for record in records:
                        print(record[feature_to_print])
            elif 'order by' in words:
                order_feature = words[words.index('order by') + 1]  # Extract  order by words
                order = 'ASC'  # Default Asc
                if 'DESC' in words:
                    order = 'DESC'
                feature_to_print = words[1]  #
                ordered_records = order_by(order_feature, database_directory, order)
                # Print output
                for record in ordered_records:
                    print(record[feature_to_print])

                else:
                    print(f"There are {result} unique values in the feature {feature}.")

            elif 'the average value of' in command:
                operation = 'the average value of'
                feature = command.split(operation)[1].strip()
                print(feature)
                if feature not in ['YEAR', 'HOUR']:
                    print(f"Aggregation operation is not supported for {feature}")
                else:
                    result = aggregate('avg', feature, database_directory)
                    print(f"The average of {feature} is: {result}")

            elif 'the max value of' in command:
                operation = 'the max value of'
                feature = command.split(operation)[1].strip()
                if feature not in ['YEAR', 'HOUR']:
                    print(f"Aggregation operation is not supported for {feature}")
                else:
                    result = aggregate('max', feature, database_directory)
                    print(f"The max of {feature} is: {result}")

            elif 'the min value of' in command:
                operation = 'the min value of'
                feature = command.split(operation)[1].strip()
                if feature not in ['YEAR', 'HOUR']:
                    print(f"Aggregation operation is not supported for {feature}")
                else:
                    result = aggregate('min', feature, database_directory)
                    print(f"The min of {feature} is: {result}")

            elif 'how many unique values in' in command:
                feature = command.split('how many unique values in')[1].strip()
                result = count_unique(feature, database_directory)
                if feature == 'dataset':
                    print(f"There are {result} unique files in the dataset.")
                else:
                    print(f"There are {result} unique values in the feature {feature}.")

            else:
                print("Invalid query format for projection.")


        else:
            print(f"Unknown command: {command}")


if __name__ == "__main__":
    print("""
    ____             __              ______     _                     ____  ____ 
   / __ )____  _____/ /_____  ____  / ____/____(_)___ ___  ___       / __ \/ __ )
  / __  / __ \/ ___/ __/ __ \/ __ \/ /   / ___/ / __ `__ \/ _ \     / / / / __  |
 / /_/ / /_/ (__  ) /_/ /_/ / / / / /___/ /  / / / / / / /  __/    / /_/ / /_/ / 
/_____/\____/____/\__/\____/_/ /_/\____/_/  /_/_/ /_/ /_/\___/    /_____/_____/                                                    
                                                                                                                                                                                                
    """)
    main()

Cx152328