import csv
from username_generator import generate_usernames

sic_codes = {
    'dev': ['43390', '43210', '62090','62020'], 
    'prod': ['41202', '41201', '42110', '42120', '42130', '42210', '42220', '42910', '42990', '43210', '43220', '43290', '43110', '43120', '43130', '43991', '43310', '43320', '43330', '43341', '43342', '43910', '43390', '43999']
}

current_env = 'prod'

def read_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def should_process_line(line):
    # Check the quick conditions first
    if line['CompanyStatus'] != 'Active':
        return False
    if line['Accounts.AccountCategory'] == 'DORMANT':
        return False
    if line['CompanyCategory'] != 'Private Limited Company':
        return False

    # Since above conditions are satisfied, now check SIC codes
    # Assuming 'sic_codes' and 'current_env' are imported from 'env_config.py'
    relevant_sic_codes = sic_codes[current_env]
    sic_code_fields = ['SICCode.SicText_1', 'SICCode.SicText_2', 'SICCode.SicText_3', 'SICCode.SicText_4']

    # Check if any SIC code matches
    for field in sic_code_fields:
        if line[field] and any(line[field].startswith(sic_code) for sic_code in relevant_sic_codes):
            return True

    return False

def write_to_csv(file_path, data):
    if not data:
        return  # No data to write

    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        # Dynamically extract fieldnames from the first item in the data list
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def main(input_csv, output_csv):
    data_to_process = read_csv(input_csv)
    processed_data = []

    for line in data_to_process:
        if should_process_line(line):
            # Mapping and renaming specific columns
            
            new_line = {
                'company_name': line['CompanyName'],
                'company_number': line[' CompanyNumber'].strip(),  # Removing leading/trailing spaces
                'incorporation_date': line['IncorporationDate'],
                'accounts_category': line['Accounts.AccountCategory'],
                'sic_1': line['SICCode.SicText_1'],
                'sic_2': line['SICCode.SicText_2'],
                'sic_3': line['SICCode.SicText_3'],
                'sic_4': line['SICCode.SicText_4']
            }

            # Generate usernames and add to the new_line
            company_name = new_line['company_name']
            username_score_pairs = generate_usernames(company_name)
            for username, score in username_score_pairs:
                # Copy new_line and add username and score
                username_line = new_line.copy()
                username_line['username'] = username
                username_line['score'] = score
                processed_data.append(username_line)
    
    write_to_csv(output_csv, processed_data)


if __name__ == "__main__":
    main('.\\data\\prod\\bronze\\BasicCompanyDataAsOneFile-2023-11-01\\BasicCompanyDataAsOneFile-2023-11-01.csv', '.\\data\\prod\\bronze\\BasicCompanyDataAsOneFile-2023-11-01\\out_usernames_companies.csv')
    # main('.\\data\\dev\\bronze\\companies.csv', '.\\data\\dev\\bronze\\out_usernames_companies.csv')
