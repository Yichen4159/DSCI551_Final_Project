# query.py
import os
import json
from datetime import datetime

def projection(incident_number, database_dir):
    json_path = os.path.join(database_dir, f"{incident_number}.json")
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            data = json.load(file)
        return data
    else:
        return f"No record found for INCIDENT_NUMBER: {incident_number}"


def filtering(feature_to_print, filter_feature, condition, value, database_dir):
    results = []
    for filename in os.listdir(database_dir):
        if filename.endswith('.json'):
            with open(os.path.join(database_dir, filename), 'r') as file:
                data = json.load(file)
                # For YEAR and HOUR
                if filter_feature in ['HOUR', 'YEAR']:
                    data_value = int(data.get(filter_feature, 0))
                    value = int(value)
                    if condition == 'is' and data_value == value:
                        results.append(data[feature_to_print])
                    elif condition == 'less' and data_value < value:
                        results.append(data[feature_to_print])
                    elif condition == 'larger' and data_value > value:
                        results.append(data[feature_to_print])
                else:
                    # For the rest of features
                    data_value = data.get(filter_feature, '')
                    if condition == 'is' and data_value == value:
                        results.append(data[feature_to_print])
    return results


def group_by(group_feature, database_dir):
    grouped_results = {}
    for filename in os.listdir(database_dir):
        if filename.endswith('.json'):
            with open(os.path.join(database_dir, filename), 'r') as file:
                data = json.load(file)
                if group_feature in data:
                    key = data[group_feature]
                    grouped_results.setdefault(key, []).append(data)
    return grouped_results


def order_by(order_feature, database_dir, order='ASC'):
    records = []
    for filename in os.listdir(database_dir):
        if filename.endswith('.json'):
            with open(os.path.join(database_dir, filename), 'r') as file:
                data = json.load(file)
                records.append(data)

    # 定义一个辅助函数，用于转换数据类型以支持排序
    def get_sort_key(record):
        value = record.get(order_feature)
        if order_feature in ['YEAR', 'HOUR']:
            return int(value)
        elif order_feature == 'DATE':
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value

    # 对记录进行排序
    records.sort(key=get_sort_key, reverse=(order == 'DESC'))

    return records


# query.py
def aggregate(operation, feature, database_dir):
    values = []
    for filename in os.listdir(database_dir):
        if filename.endswith('.json'):
            with open(os.path.join(database_dir, filename), 'r') as file:
                data = json.load(file)
                if feature in data:
                    # Make sure the datatype is int
                    values.append(int(data[feature]))

    if operation == 'avg':
        return sum(values) / len(values) if values else 0
    elif operation == 'min':
        return min(values) if values else None
    elif operation == 'max':
        return max(values) if values else None
    else:
        raise ValueError("Invalid aggregation operation")


def count_unique(feature, database_dir):
    unique_values = set()
    if feature == 'dataset':
        for filename in os.listdir(database_dir):
            if filename.endswith('.json'):
                unique_values.add(filename)
    else:
        for filename in os.listdir(database_dir):
            if filename.endswith('.json'):
                with open(os.path.join(database_dir, filename), 'r') as file:
                    data = json.load(file)
                    if feature in data:
                        unique_values.add(data[feature])

    return len(unique_values)


