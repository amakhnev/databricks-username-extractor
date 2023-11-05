# Databricks Username Extractor

## Introduction
Welcome to the Databricks Username Extractor project! This is a fun and practical project designed for playing around with Databricks. Our aim is to help a fictional B2B web application that requires unique usernames. We're tackling the challenge of username squatting by making sure usernames that match existing company names aren't grabbed unfairly. How? By using data from the UK Companies House to extract the names of active companies and generating potential usernames to block public registration. This way, real companies can later claim their usernames with ease.

## Task
The goals which is in scope: 
1. Extract the names of active companies from the UK Companies House data.
2. Generate a list of potential usernames based on company names.
3. Ensure name variations are covered, including acronyms
4. Make an analysys and find out how likely the company would occupy the username. 

## Project Structure
The project is organized into a few key folders:

- `data`: This is where the UK Companies House data and any other data files we use are stored.
- `libraries`: Here you'll find custom libraries or modules designed to be testable and reusable.
- `notebooks`: Contains Databricks notebooks with all our code and analysis.


## How to Run Tests Locally
To run tests on your local machine, follow these steps:

1. Make sure you have a Python environment set up.
2. Install the necessary dependencies, which are listed in the `requirements.txt` file.
3. Navigate to the project directory in your terminal or command prompt.
4a. Run the test command 
```sh
 python -m unittest discover -s ./tests -p "test_*.py" 
```
.
4b. if you use Visual Studio Code - use testing panel


## How to Import into Databricks
To get this project into Databricks, you can follow these steps:

1. Log in to your Databricks account.
2. Navigate to the workspace where you want to import the project.
3. Click on the 'Create' button and select 'Import'.
4. You can then upload the Databricks notebooks directly if you have them as files, or you can import them from a URL if the notebooks are hosted in a GitHub repository.
5. For the `data` and `libraries`, you can upload these to your Databricks File System (DBFS) using the Databricks CLI or directly through the workspace UI.

And that's it! Once everything's uploaded, you're ready to start playing with the data and running your analyses.

