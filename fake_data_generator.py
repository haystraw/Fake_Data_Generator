import csv
import random
from faker import Faker
import jaydebeapi
import jpype
import jpype.imports
import sys
import os
import argparse
import ast
from datetime import date, datetime

version = 20250331

default_minimum_rows = 10
default_maximum_rows = 100

# Start the JVM with the path to your Oracle JDBC driver
if not jpype.isJVMStarted():
    jpype.startJVM(classpath=['./ojdbc11.jar'])

# Database connection configuration
jdbc_url = 'jdbc:oracle:thin:@//infadomain4.mxdomain:1521/orcl'
driver_class = 'oracle.jdbc.OracleDriver'
username = 'LOAN'
password = 'LOAN'

# Import the Java SQL Date class
from java.sql import Date as JavaSqlDate

# Initialize Faker instance
fake = Faker()

unique_id_set = {}


script_location = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))

help_message = '''
Creates fake data within an existing JDBC source.
    --config_file_name Specify the name of the csv config file to use. Otherwise it will prompt you

'''

def to_java_date(py_date):
    """Convert a Python date to a Java SQL Date."""
    if isinstance(py_date, date):
        datetime_obj = datetime.combine(py_date, datetime.min.time())
        epoch = datetime(1970, 1, 1)
        milliseconds = int((datetime_obj - epoch).total_seconds() * 1000)
        return JavaSqlDate(milliseconds)
    else:
        raise ValueError("Expected a `date` object")

def get_existing_ids(table_name, column_name):
    """Fetch existing primary key IDs to avoid duplicates."""
    print(f"INFO: Fetching existing ids from {table_name}.{column_name}")
    query = f"SELECT {column_name} FROM {table_name}"
    cursor.execute(query)
    return set(row[0] for row in cursor.fetchall())

def existing_id(table_name, id_column_name):
    table_column = table_name+"."+id_column_name
    if table_column not in unique_id_set:
        unique_id_set[table_column] = get_existing_ids(table_name, id_column_name)
     
    this_id = random.choice(list(unique_id_set[table_column]))
    return this_id

def unique_id(table_name, id_column_name, min_value, max_value):
    table_column = table_name+"."+id_column_name
    if table_column not in unique_id_set:
        unique_id_set[table_column] = get_existing_ids(table_name, id_column_name)

    this_id = fake.unique.random_int(min=min_value, max=max_value)
    while this_id in unique_id_set[table_column]:
        this_id = fake.unique.random_int(min=min_value, max=max_value)
        
    unique_id_set[table_column].add(this_id)
    ## print(f"DEBUG unique_id is {this_id}")
    return this_id
        
    
    

def generate_realistic_email(first_name, last_name, domains=['gmail.com', 'yahoo.com', 'hotmail.com', 'example.com']):
    domain = fake.random_element(elements=(domains))
    # Construct the email address
    first_name_initial = first_name[0]
    last_name_initial = last_name[0]
    rand_num = fake.random_int(min=1, max=99)
    
    emails = []
    emails.append(f"{first_name}.{last_name}@{domain}")
    emails.append(f"{first_name}_{last_name}@{domain}")
    emails.append(f"{last_name}@{domain}")
    emails.append(f"{first_name_initial}.{last_name}@{domain}")
    emails.append(f"{first_name_initial}{last_name}@{domain}")
    emails.append(f"{first_name_initial}_{last_name}@{domain}")
    emails.append(f"{first_name}.{last_name}{rand_num}@{domain}")
    emails.append(f"{first_name}_{last_name}{rand_num}@{domain}")
    emails.append(f"{last_name}{rand_num}@{domain}")
    emails.append(f"{first_name_initial}.{last_name}{rand_num}@{domain}")
    emails.append(f"{first_name_initial}{last_name}{rand_num}@{domain}")
    emails.append(f"{first_name_initial}_{last_name}{rand_num}@{domain}")
    emails.append(f"{first_name}.{last_name}_{rand_num}@{domain}")
    emails.append(f"{first_name}_{last_name}_{rand_num}@{domain}")
    emails.append(f"{last_name}_{rand_num}@{domain}")
    emails.append(f"{first_name_initial}.{last_name}_{rand_num}@{domain}")
    emails.append(f"{first_name_initial}{last_name}_{rand_num}@{domain}")
    emails.append(f"{first_name_initial}_{last_name}_{rand_num}@{domain}")
    email = fake.random_element(elements=(emails))
    return email



# Load the CSV configuration
def load_csv_configuration(file_path):
    config = {}
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            table_column = (row['Table'], row['Column'])
            config[table_column] = row['DataFunction']
    return config

# Function to insert fake data dynamically based on CSV configuration
def insert_fake_data(config, table_name, num_rows):
    table_columns = [col for (tbl, col) in config.keys() if tbl == table_name]
    placeholder = ', '.join(['?'] * len(table_columns))
    
    for count in range(num_rows):
        values = []
        
        # Local dynamically evaluated context
        local_vars = {"fake": fake, "random": random, "generate_realistic_email": generate_realistic_email, "unique_id": unique_id, "existing_id": existing_id, "round": round}
        
        for column in table_columns:
            func_str = config[(table_name, column)]
            try:
                # Evaluate the function string
                ## print(f"Evaluating function for {table_name}.{column}: {func_str}")
                value = eval(func_str, {"__builtins__": None}, local_vars)
                
                local_vars[column] = value
                
                if isinstance(value, date):
                    value = to_java_date(value)
                values.append(value)
                
            except Exception as e:
                print(f"Error generating data for {table_name}.{column}: {e}")
                values.append(None)

        query = f"INSERT INTO {table_name} ({', '.join(table_columns)}) VALUES ({placeholder})"
        cursor.execute(query, values)
        
        print(f"\rTable: {table_name} | inserted: {count + 1}/{num_rows}", end="")
        sys.stdout.flush()
    print()  # newline after loop



def get_tables(config_file):
    unique_list = []
    seen_items = set()
    with open(config_file, newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile)

        # Iterate over each row in the file
        for row in csv_reader:
            item = row['Table']
            # Only add item if it's not yet seen
            if item not in seen_items:
                this_table_name = row['Table']
                
                this_minimum_rows = default_minimum_rows
                this_maximum_rows = default_maximum_rows
                try:
                    this_minimum_rows = int(row['RowNumMinimum'])
                    this_maximum_rows = int(row['RowNumMaximum'])
                except:
                    pass
                this_rows = random.randint(this_minimum_rows, this_maximum_rows)
                this_obj = {"table_name": this_table_name, "rows": this_rows}
                unique_list.append(this_obj)
                seen_items.add(item)

    return unique_list

def parse_parameters():
    # Check for --help first
    if '--help' in sys.argv:
        print(help_message)
        programPause = input("Press the <ENTER> key to exit...")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Dynamically set variables from command-line arguments.")
    args, unknown_args = parser.parse_known_args()

    for arg in unknown_args:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)  # Remove "--" and split into key and value
            try:
                # Safely parse value as Python object (list, dict, etc.)
                value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass  # Leave value as-is if parsing fails

            # Handle appending to arrays or updating dictionaries
            if key in globals():
                existing_value = globals()[key]
                if isinstance(existing_value, list) and isinstance(value, list):
                    ## If what was passed is an array, we'll append to the array
                    existing_value.extend(value)  # Append to the existing array
                elif isinstance(existing_value, dict) and isinstance(value, dict):
                    ## If what was passed is a dict, we'll add to the dict
                    existing_value.update(value)  # Add or update keys in the dictionary
                else:
                    ## Otherwise, it's an ordinary variable. replace it
                    globals()[key] = value  # Replace for other types
            else:
                ## It's a new variable. Create an ordinary variable.
                globals()[key] = value  # Set as a new variable

def select_recent_csv(directory):
    """
    Lists CSV files in a given directory, sorted by most recent modification time,
    prompts the user to select one, and returns the path for the selected file.

    Args:
        directory (str): The directory to search for CSV files.

    Returns:
        str: The full path of the selected CSV file, or None if no valid file is selected.
    """
    # Expand user directory if ~ is used
    directory = os.path.expanduser(directory)

    # Check if the directory exists
    if not os.path.isdir(directory):
        print(f"Directory not found: {directory}")
        return None

    # List all CSV files in the directory
    csv_files = [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if file.endswith('.csv')
    ]

    # Check if any CSV files were found
    if not csv_files:
        print(f"No CSV files found in the directory: {directory}")
        return None

    # Sort the files by modification time (most recent first)
    csv_files.sort(key=os.path.getmtime, reverse=True)

    # Display the files to the user with their modification times
    print("Select a CSV file:")
    for i, file in enumerate(csv_files, start=1):
        ## mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file))
        print(f"    {i}. {os.path.basename(file)}")

    # Prompt the user to select a file
    while True:
        try:
            choice = int(input(f"Enter the number of the file to select (1-{len(csv_files)}): "))
            if 1 <= choice <= len(csv_files):
                selected_file = csv_files[choice - 1]
                return selected_file
            else:
                print(f"Invalid choice. Please select a number between 1 and {len(csv_files)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def main():
    print(f"INFO: Version {version}")
    global config_file_name
    config_file_name = 'xxxxxxx'
    parse_parameters()
    
    # Establish JDBC connection
    connection = jaydebeapi.connect(driver_class, jdbc_url, [username, password])
    connection.jconn.setAutoCommit(False)
    global cursor
    cursor = connection.cursor()

    if not os.path.isfile(config_file_name):
        config_file_name = select_recent_csv(script_location)




    for obj in get_tables(config_file_name):
        table = obj["table_name"]
        number_of_rows = obj["rows"]
        csv_config = load_csv_configuration(config_file_name)
        insert_fake_data(csv_config, table, number_of_rows)

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()    
