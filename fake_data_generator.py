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
from dateutil.relativedelta import relativedelta

version = 20250401

default_minimum_rows = 10
default_maximum_rows = 100

## Just cheating to deal with timezones, when creating datetimes
## Adjust if setting a date is getting off
hours_offset = 6

## When specifying a date before or after, what's the default range
default_years_before = 10
default_years_after = 10

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
Creates fake data within an existing JDBC source. Modify the top of the script to provide your connection details, 
    including the JDBC jar location.

    As an input it will take a csv file which defines where you want to create fake data, and what methods to use.
    This Csv file has the following headers:
    - Table: Table name to insert into (it assumes the table already exists)
    - Column: Column for to add data into (it assumes the column within the table already exists)
    - DataFunction: This can be several different methods:
        fake: Uses the Python "Faker" library. Examples are:
            fake.first_name()
            fake.last_name()
            fake.date_of_birth(tzinfo=None, minimum_age=18, maximum_age=70)
            fake.numerify('###-###-####')
            fake.street_address()
            fake.city()
            fake.state_abbr()
            fake.random_element(elements=('USA', 'Canada', 'UK', 'Australia', 'Germany', 'India'))
            fake.ssn()
            fake.random_int(min=10000, max=500000)

        random: This uses the Python random library to randomly generate a number, or other choice. Examples are:
            random.uniform(2.0, 6.0)

        round: Usually used in conjunction with random to round a number. Examples are:
            round(random.uniform(2.0, 6.0), 2)

        unique_id: Custom function that will generate a random integer between a min and maximum, but
                   will also ensure that it is unique from within a Table, column specified. This ensures
                   that it will not only be unique during the run, but will accomodate existing data.
                   When using, you specify the name of tha Table and column to check, and the min and max values to use.
                   Examples are:
            unique_id('Payments', 'Payment_ID', 1, 9999999)
            unique_id('Credit_Scores', 'Credit_Score_ID', 1, 9999999)

        existing_id: Custom function that will randomly select an id from a different column. 
                     Useful for Foreign key inegrety. When using, you specify the name of the Table, Column to pull from.
                     Examples are:
            existing_id('Borrowers', 'Borrower_ID')
            existing_id('Loans', 'Loan_ID')

        based_on_value: Custom function that will create a value based off another value. It will run a query of a table, 
                        where the "column specified" = "value specified", and return the "actionable column specified", then
                        it will generate a value greater then, less than, or equal to that value.
                        Note that you can specify other columns you generated as the "value" if desired.
                        When using, use specify the Table, Column, Value, operator ("<", ">", or "="), and the actionable column.
                        This can accomodate number, date/datetime, and string (in the case of "="), if it is a date, specify date=True
                        Examples are:
                based_on_value('Loans', 'Loan_ID', Loan_ID, '>', 'loan_amount')
                    In this example, it will query: select loan_amount from Loans where Loan_ID = <whatever the recently created Loan_ID value was>
                    and then generate an integer that is greater than that loan_amount
                based_on_value('Loans', 'Loan_ID', Loan_ID, '=', 'start_date', date=True)
                    In this example, it will query: select start_date from Loans where Loan_ID = <whatever the recently created Loan_ID value was>
                    and then generate an date that is equal to that start_date

        generate_realistic_email: Custom function that will create an email, based off a First and Last Name provided. Optionally, you can 
                                  provide domains to use, otherwise it'll default to using: 'gmail.com', 'yahoo.com', 'hotmail.com', 'example.com'
                                  The values of First and Last name can be other columns that you've generated
                                  It will randomly create emails in formats like:
                                    Scott_Hayes@domain
                                    Scott.Hayes@domain
                                    SHayes@domain
                                    Hayes@domain
                                    SHayes234@domain
                                    etc. 
            generate_realistic_email(First_Name, Last_Name)
                This will create an email using the previously created fields of "First_Name", and "Last_Name"
            generate_realistic_email(First_Name, Last_Name, domains=['myemail.com', 'kob.org', 'microsfot.com'])
                This will create an email using the previously created fields of "First_Name", and "Last_Name"
                And will only use the specified domains

Optional Command Line arguments
    --config_file_name Specify the name of the csv config file to use. Otherwise it will prompt you for which csv file to use.
        Example: --config_file_name="Loan Data (big load).csv"

'''
def to_java_date(py_date):
    """Convert a Python date to a Java SQL Date."""
    if isinstance(py_date, date):
        datetime_obj = datetime.combine(py_date, datetime.min.time())
        epoch = datetime(1970, 1, 1)
        milliseconds = int((datetime_obj - epoch).total_seconds() * 1000)+(hours_offset*60*60*1000)
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
        
def fetch_value_based_on_id(table_name, id_column_name, value_column_name):
    """Fetch existing primary key IDs with their corresponding values."""
    print(f"INFO: Fetching {id_column_name}, {value_column_name} from {table_name}")
    query = f"SELECT {id_column_name}, {value_column_name} FROM {table_name}"
    cursor.execute(query)
    # Use a dictionary comprehension to create a dictionary based on the query result
    result_dict = {row[0]: row[1] for row in cursor.fetchall()}
    
    return result_dict

def based_on_value(table_name, id_column_name, id_value, operator, value_column_name, date=False):
    table_column = table_name+"."+id_column_name+"."+value_column_name
    if table_column not in unique_id_set:
        unique_id_set[table_column] = fetch_value_based_on_id(table_name, id_column_name, value_column_name)
    this_set = unique_id_set[table_column]
    this_value = this_set[id_value]
    if date:
        try:
            this_value = datetime.strptime(this_value, "%Y-%m-%d").date()
        except:
            this_value = datetime.strptime(this_value, "%Y-%m-%d %H:%M:%S")
        date_years_before = this_value - relativedelta(years=default_years_before)
        date_years_after = this_value + relativedelta(years=default_years_after)
    if operator in ("lt", "<", "LT", "LessThan", "Less Than"):
        if date:
            return fake.date_between_dates(date_start=date_years_before, date_end=this_value)
        else:
            return fake.random_int(min=1, max=this_value)
    elif operator in ("gt", ">", "GT", "GreaterThan", "Greater Than"):
        if date:
            return fake.date_between_dates(date_start=this_value, date_end=date_years_after)
        else:
            return fake.random_int(min=this_value, max=this_value+1000000)
    else:
        return this_value
       

    

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
        local_vars = {"fake": fake, 
                      "random": random, 
                      "generate_realistic_email": generate_realistic_email, 
                      "unique_id": unique_id, 
                      "existing_id": existing_id, 
                      "round": round,
                      "random": random,
                      "based_on_value": based_on_value
                      }
        
        for column in table_columns:
            func_str = config[(table_name, column)]
            try:
                # Evaluate the function string
                ## print(f"Evaluating function for {table_name}.{column}: {func_str}")
                value = eval(func_str, {"__builtins__": None}, local_vars)

                if isinstance(value, date):
                    value = to_java_date(value)
                values.append(value)

                local_vars[column] = value
                
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
    print(f"INFO: Connecting to {jdbc_url}")
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
