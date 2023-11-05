import re

def generate_usernames(company_name):
    # List of UK incorporation terms to ignore
    incorporation_terms = ['limited', 'ltd', 'llp', 'plc', 'group', 'holdings', '(uk)']
    
    # Clean the company name
    clean_name = company_name.lower()
    for term in incorporation_terms:
        clean_name = clean_name.replace(term, '')
    
    # Remove any non-allowed characters and replace spaces with underscores
    clean_name = re.sub(r'[^a-z0-9 ]', '', clean_name)  # Keep only lowercase, numbers, and space
    clean_name = re.sub(r'\s+', ' ', clean_name.strip())  # Replace one or more spaces with a single space

    # Generate acronyms
    acronym = ''.join(part[0] for part in clean_name.split() if part)
    potential_usernames = {acronym} if 3 <= len(acronym) <= 20 else set()

    # Generate potential usernames by splitting the cleaned name and combining parts
    name_parts = clean_name.split()
    for i in range(len(name_parts)):
        for j in range(i+1, len(name_parts) + 1):
            username = '_'.join(name_parts[i:j])
            if 3 <= len(username) <= 20:  # Check the length constraints
                potential_usernames.add(username)
                if '_' in username:
                    # Adding acronym variations by replacing parts with their initials
                    for k in range(i, j):
                        temp_parts = name_parts[i:j]  # Create a new sublist
                        temp_username = "_".join([part[0] if idx == k else part for idx, part in enumerate(temp_parts)])
                        if 3 <= len(temp_username) <= 20:
                            potential_usernames.add(temp_username)

    # Relevance score calculations
    usernames_with_scores = []
    clean_name_without_spaces = clean_name.replace(' ', '')
    for username in potential_usernames:
        if username == clean_name_without_spaces or username == acronym:
            score = 100
        else:
            # Calculate percentage of username in the company name
            count = clean_name_without_spaces.count(username.replace('_', ''))
            score = (count * len(username.replace('_', '')) / len(clean_name_without_spaces)) * 100
        usernames_with_scores.append((username, round(score)))

    # Sort by relevance score and limit to 5 usernames
    usernames_with_scores.sort(key=lambda x: x[1], reverse=True)
    return usernames_with_scores[:5]