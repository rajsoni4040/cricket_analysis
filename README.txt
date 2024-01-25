Cricket Data Processing (cricket.ipynb)
This Jupyter Notebook is designed to process cricket match data from JSON files and store it in a SQLite database. The provided code includes functions for creating the necessary database tables, inserting player information, match details, innings data, and deliveries data.


Make sure you have the following dependencies installed:

Jupyter Notebook
Python 3.x
SQLite


To install the required Python packages, you can run:
pip install -r requirements.txt


Usage
Clone the repository or download the notebook (cricket.ipynb).
Open the Jupyter Notebook in your local environment.
Ensure you have the required dependencies installed.
Run the notebook cells to execute the code.
Database Schema
The notebook creates the following tables in the SQLite database:

players: Stores player information.
matches: Stores details about cricket matches.
innings: Records information about each inning in a match.
deliveries: Contains details about each delivery in a match.
The database schema is defined in the create_database_tables function within the notebook.